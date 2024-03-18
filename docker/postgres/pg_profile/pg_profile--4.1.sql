\echo Use "CREATE EXTENSION pg_profile" to load this file. \quit
/* ========= Core tables ========= */

CREATE TABLE servers (
    server_id           SERIAL PRIMARY KEY,
    server_name         name UNIQUE NOT NULL,
    server_description  text,
    server_created      timestamp with time zone DEFAULT now(),
    db_exclude          name[] DEFAULT NULL,
    enabled             boolean DEFAULT TRUE,
    connstr             text,
    max_sample_age      integer NULL,
    last_sample_id      integer DEFAULT 0 NOT NULL,
    size_smp_wnd_start  time with time zone,
    size_smp_wnd_dur    interval hour to second,
    size_smp_interval   interval day to minute
);
COMMENT ON TABLE servers IS 'Monitored servers (Postgres clusters) list';

CREATE TABLE samples (
    server_id integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id integer NOT NULL,
    sample_time timestamp (0) with time zone,
    CONSTRAINT pk_samples PRIMARY KEY (server_id, sample_id)
);

CREATE INDEX ix_sample_time ON samples(server_id, sample_time);
COMMENT ON TABLE samples IS 'Sample times list';

CREATE TABLE baselines (
    server_id   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    bl_id       SERIAL,
    bl_name     varchar (25) NOT NULL,
    keep_until  timestamp (0) with time zone,
    CONSTRAINT pk_baselines PRIMARY KEY (server_id, bl_id),
    CONSTRAINT uk_baselines UNIQUE (server_id,bl_name)
);
COMMENT ON TABLE baselines IS 'Baselines list';

CREATE TABLE bl_samples (
    server_id   integer NOT NULL,
    sample_id   integer NOT NULL,
    bl_id       integer NOT NULL,
    CONSTRAINT fk_bl_samples_samples FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT,
    CONSTRAINT fk_bl_samples_baselines FOREIGN KEY (server_id, bl_id) REFERENCES baselines(server_id, bl_id) ON DELETE CASCADE,
    CONSTRAINT pk_bl_samples PRIMARY KEY (server_id, bl_id, sample_id)
);
CREATE INDEX ix_bl_samples_blid ON bl_samples(bl_id);
COMMENT ON TABLE bl_samples IS 'Samples in baselines';
/* ==== Clusterwide stats history tables ==== */

CREATE TABLE sample_stat_cluster
(
    server_id                  integer,
    sample_id                  integer,
    checkpoints_timed          bigint,
    checkpoints_req            bigint,
    checkpoint_write_time      double precision,
    checkpoint_sync_time       double precision,
    buffers_checkpoint          bigint,
    buffers_clean               bigint,
    maxwritten_clean           bigint,
    buffers_backend             bigint,
    buffers_backend_fsync       bigint,
    buffers_alloc               bigint,
    stats_reset                timestamp with time zone,
    wal_size                   bigint,
    CONSTRAINT fk_statcluster_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_cluster PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_cluster IS 'Sample cluster statistics table (fields from pg_stat_bgwriter, etc.)';

CREATE TABLE last_stat_cluster AS SELECT * FROM sample_stat_cluster WHERE 0=1;
ALTER TABLE last_stat_cluster ADD CONSTRAINT fk_last_stat_cluster_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_cluster IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_wal
(
    server_id           integer,
    sample_id           integer,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    wal_buffers_full    bigint,
    wal_write           bigint,
    wal_sync            bigint,
    wal_write_time      double precision,
    wal_sync_time       double precision,
    stats_reset         timestamp with time zone,
    CONSTRAINT fk_statwal_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_wal PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_wal IS 'Sample WAL statistics table';

CREATE TABLE last_stat_wal AS SELECT * FROM sample_stat_wal WHERE false;
ALTER TABLE last_stat_wal ADD CONSTRAINT fk_last_stat_wal_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_wal IS 'Last WAL sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_archiver
(
    server_id                   integer,
    sample_id                   integer,
    archived_count              bigint,
    last_archived_wal           text,
    last_archived_time          timestamp with time zone,
    failed_count                bigint,
    last_failed_wal             text,
    last_failed_time            timestamp with time zone,
    stats_reset                 timestamp with time zone,
    CONSTRAINT fk_sample_stat_archiver_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_archiver PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_archiver IS 'Sample archiver statistics table (fields from pg_stat_archiver)';

CREATE TABLE last_stat_archiver AS SELECT * FROM sample_stat_archiver WHERE 0=1;
ALTER TABLE last_stat_archiver ADD CONSTRAINT fk_last_stat_archiver_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_archiver IS 'Last sample data for calculating diffs in next sample';
/* ==== Tablespaces stats history ==== */
CREATE TABLE tablespaces_list(
    server_id           integer REFERENCES servers(server_id) ON DELETE CASCADE,
    tablespaceid        oid,
    tablespacename      name NOT NULL,
    tablespacepath      text NOT NULL, -- cannot be changed without changing oid
    last_sample_id      integer,
    CONSTRAINT pk_tablespace_list PRIMARY KEY (server_id, tablespaceid),
    CONSTRAINT fk_tablespaces_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
CREATE INDEX ix_tablespaces_list_smp ON tablespaces_list(server_id, last_sample_id);
COMMENT ON TABLE tablespaces_list IS 'Tablespaces, captured in samples';

CREATE TABLE sample_stat_tablespaces
(
    server_id           integer,
    sample_id           integer,
    tablespaceid        oid,
    size                bigint NOT NULL,
    size_delta          bigint NOT NULL,
    CONSTRAINT fk_stattbs_samples FOREIGN KEY (server_id, sample_id)
        REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_st_tablespaces_tablespaces FOREIGN KEY (server_id, tablespaceid)
        REFERENCES tablespaces_list(server_id, tablespaceid)
        ON DELETE NO ACTION ON UPDATE CASCADE
        DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pk_sample_stat_tablespaces PRIMARY KEY (server_id, sample_id, tablespaceid)
);
CREATE INDEX ix_sample_stat_tablespaces_ts ON sample_stat_tablespaces(server_id, tablespaceid);

COMMENT ON TABLE sample_stat_tablespaces IS 'Sample tablespaces statistics (fields from pg_tablespace)';

CREATE VIEW v_sample_stat_tablespaces AS
    SELECT
        server_id,
        sample_id,
        tablespaceid,
        tablespacename,
        tablespacepath,
        size,
        size_delta
    FROM sample_stat_tablespaces JOIN tablespaces_list USING (server_id, tablespaceid);
COMMENT ON VIEW v_sample_stat_tablespaces IS 'Tablespaces stats view with tablespace names';

CREATE TABLE last_stat_tablespaces (LIKE v_sample_stat_tablespaces)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_tablespaces IS 'Last sample data for calculating diffs in next sample';
CREATE TABLE roles_list(
    server_id       integer REFERENCES servers(server_id) ON DELETE CASCADE,
    userid          oid,
    username        name NOT NULL,
    last_sample_id  integer,
    CONSTRAINT pk_roles_list PRIMARY KEY (server_id, userid),
    CONSTRAINT fk_roles_list_smp FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
);
CREATE INDEX ix_roles_list_smp ON roles_list(server_id, last_sample_id);

COMMENT ON TABLE roles_list IS 'Roles, captured in samples';
/* ==== Database stats history tables === */

CREATE TABLE sample_stat_database
(
    server_id           integer,
    sample_id           integer,
    datid               oid,
    datname             name NOT NULL,
    xact_commit         bigint,
    xact_rollback       bigint,
    blks_read           bigint,
    blks_hit            bigint,
    tup_returned        bigint,
    tup_fetched         bigint,
    tup_inserted        bigint,
    tup_updated         bigint,
    tup_deleted         bigint,
    conflicts           bigint,
    temp_files          bigint,
    temp_bytes          bigint,
    deadlocks           bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    stats_reset         timestamp with time zone,
    datsize             bigint,
    datsize_delta       bigint,
    datistemplate       boolean,
    session_time        double precision,
    active_time         double precision,
    idle_in_transaction_time  double precision,
    sessions            bigint,
    sessions_abandoned  bigint,
    sessions_fatal      bigint,
    sessions_killed     bigint,
    checksum_failures   bigint,
    checksum_last_failure timestamp with time zone,
    CONSTRAINT fk_statdb_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_database PRIMARY KEY (server_id, sample_id, datid)
);
COMMENT ON TABLE sample_stat_database IS 'Sample database statistics table (fields from pg_stat_database)';

CREATE TABLE last_stat_database (LIKE sample_stat_database)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_database IS 'Last sample data for calculating diffs in next sample';
/* ==== Tables stats history ==== */
CREATE TABLE tables_list(
    server_id           integer REFERENCES servers(server_id) ON DELETE CASCADE,
    datid               oid,
    relid               oid,
    relkind             char(1) NOT NULL,
    reltoastrelid       oid,
    schemaname          name NOT NULL,
    relname             name NOT NULL,
    last_sample_id      integer,
    CONSTRAINT pk_tables_list PRIMARY KEY (server_id, datid, relid),
    CONSTRAINT fk_tables_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_toast_table FOREIGN KEY (server_id, datid, reltoastrelid)
      REFERENCES tables_list (server_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT uk_toast_table UNIQUE (server_id, datid, reltoastrelid)
);
CREATE INDEX ix_tables_list_samples ON tables_list(server_id, last_sample_id);
COMMENT ON TABLE tables_list IS 'Table names and schemas, captured in samples';

CREATE TABLE sample_stat_tables (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    tablespaceid        oid NOT NULL,
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
    relpages_bytes      bigint,
    relpages_bytes_diff bigint,
    CONSTRAINT pk_sample_stat_tables PRIMARY KEY (server_id, sample_id, datid, relid),
    CONSTRAINT fk_st_tables_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tables_tablespace FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tables_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX is_sample_stat_tables_ts ON sample_stat_tables(server_id, sample_id, tablespaceid);
CREATE INDEX ix_sample_stat_tables_rel ON sample_stat_tables(server_id, datid, relid);

COMMENT ON TABLE sample_stat_tables IS 'Stats increments for user tables in all databases by samples';

CREATE VIEW v_sample_stat_tables AS
    SELECT
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
        relpages_bytes,
        relpages_bytes_diff
    FROM sample_stat_tables JOIN tables_list USING (server_id, datid, relid);
COMMENT ON VIEW v_sample_stat_tables IS 'Tables stats view with table names and schemas';

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

CREATE TABLE sample_stat_tables_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    tablespaceid        oid,
    relkind             char(1) NOT NULL,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
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
    relsize_diff        bigint,
    CONSTRAINT pk_sample_stat_tables_tot PRIMARY KEY (server_id, sample_id, datid, relkind, tablespaceid),
    CONSTRAINT fk_st_tables_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tablespaces_tot_dat FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE
);
CREATE INDEX ix_sample_stat_tables_total_ts ON sample_stat_tables_total(server_id, sample_id, tablespaceid);

COMMENT ON TABLE sample_stat_tables_total IS 'Total stats for all tables in all databases by samples';
/* ==== Indexes stats tables ==== */
CREATE TABLE indexes_list(
    server_id       integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    datid           oid NOT NULL,
    indexrelid      oid NOT NULL,
    relid           oid NOT NULL,
    schemaname      name NOT NULL,
    indexrelname    name NOT NULL,
    last_sample_id  integer,
    CONSTRAINT pk_indexes_list PRIMARY KEY (server_id, datid, indexrelid),
    CONSTRAINT fk_indexes_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid)
        ON DELETE NO ACTION ON UPDATE CASCADE
        DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_indexes_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
CREATE INDEX ix_indexes_list_rel ON indexes_list(server_id, datid, relid);
CREATE INDEX ix_indexes_list_smp ON indexes_list(server_id, last_sample_id);

COMMENT ON TABLE indexes_list IS 'Index names and schemas, captured in samples';

CREATE TABLE sample_stat_indexes (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    indexrelid          oid,
    tablespaceid        oid NOT NULL,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize             bigint,
    relsize_diff        bigint,
    indisunique         bool,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint,
    CONSTRAINT fk_stat_indexes_indexes FOREIGN KEY (server_id, datid, indexrelid)
      REFERENCES indexes_list(server_id, datid, indexrelid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_stat_indexes_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_stat_indexes_tablespaces FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid)
      ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_indexes PRIMARY KEY (server_id, sample_id, datid, indexrelid)
);
CREATE INDEX ix_sample_stat_indexes_il ON sample_stat_indexes(server_id, datid, indexrelid);
CREATE INDEX ix_sample_stat_indexes_ts ON sample_stat_indexes(server_id, sample_id, tablespaceid);

COMMENT ON TABLE sample_stat_indexes IS 'Stats increments for user indexes in all databases by samples';

CREATE VIEW v_sample_stat_indexes AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
        tl.schemaname,
        tl.relname,
        il.indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        indisunique,
        relpages_bytes,
        relpages_bytes_diff
    FROM
        sample_stat_indexes s
        JOIN indexes_list il USING (datid, indexrelid, server_id)
        JOIN tables_list tl USING (datid, relid, server_id);
COMMENT ON VIEW v_sample_stat_indexes IS 'Reconstructed stats view with table and index names and schemas';

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

CREATE TABLE sample_stat_indexes_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    tablespaceid        oid,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize_diff         bigint,
    CONSTRAINT fk_stat_indexes_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_stat_tablespaces_tot_dat FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_indexes_tot PRIMARY KEY (server_id, sample_id, datid, tablespaceid)
);
CREATE INDEX ix_sample_stat_indexes_total_ts ON sample_stat_indexes_total(server_id, sample_id, tablespaceid);

COMMENT ON TABLE sample_stat_indexes_total IS 'Total stats for indexes in all databases by samples';
/* === Statements history tables ==== */
CREATE TABLE stmt_list(
    server_id      integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    queryid_md5    char(32),
    query          text,
    last_sample_id integer,
    CONSTRAINT pk_stmt_list PRIMARY KEY (server_id, queryid_md5),
    CONSTRAINT fk_stmt_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
CREATE INDEX ix_stmt_list_smp ON stmt_list(server_id, last_sample_id);
COMMENT ON TABLE stmt_list IS 'Statements, captured in samples';

CREATE TABLE sample_statements (
    server_id           integer,
    sample_id           integer,
    userid              oid,
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
    jit_functions       bigint,
    jit_generation_time double precision,
    jit_inlining_count  bigint,
    jit_inlining_time   double precision,
    jit_optimization_count  bigint,
    jit_optimization_time   double precision,
    jit_emission_count  bigint,
    jit_emission_time   double precision,
    CONSTRAINT pk_sample_statements_n PRIMARY KEY (server_id, sample_id, datid, userid, queryid, toplevel),
    CONSTRAINT fk_stmt_list FOREIGN KEY (server_id,queryid_md5)
      REFERENCES stmt_list (server_id,queryid_md5)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_statments_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_statements_roles FOREIGN KEY (server_id, userid)
      REFERENCES roles_list (server_id, userid)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX ix_sample_stmts_qid ON sample_statements (server_id,queryid_md5);
CREATE INDEX ix_sample_stmts_rol ON sample_statements (server_id, userid);
COMMENT ON TABLE sample_statements IS 'Sample statement statistics table (fields from pg_stat_statements)';

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

CREATE TABLE sample_statements_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    plans               bigint,
    total_plan_time     double precision,
    calls               bigint,
    total_exec_time     double precision,
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
    statements          bigint,
    jit_functions       bigint,
    jit_generation_time double precision,
    jit_inlining_count  bigint,
    jit_inlining_time   double precision,
    jit_optimization_count  bigint,
    jit_optimization_time   double precision,
    jit_emission_count  bigint,
    jit_emission_time   double precision,
    CONSTRAINT pk_sample_statements_total PRIMARY KEY (server_id, sample_id, datid),
    CONSTRAINT fk_statments_t_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_statements_total IS 'Aggregated stats for sample, based on pg_stat_statements';
CREATE TABLE wait_sampling_total(
    server_id           integer,
    sample_id           integer,
    sample_wevnt_id     integer,
    event_type          text NOT NULL,
    event               text NOT NULL,
    tot_waited          bigint NOT NULL,
    stmt_waited         bigint,
    CONSTRAINT pk_sample_weid PRIMARY KEY (server_id, sample_id, sample_wevnt_id),
    CONSTRAINT uk_sample_we UNIQUE (server_id, sample_id, event_type, event),
    CONSTRAINT fk_wait_sampling_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
);
/* ==== rusage statements history tables ==== */
CREATE TABLE sample_kcache (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    datid               oid,
    queryid             bigint,
    queryid_md5         char(32),
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
    exec_nivcsws        bigint,
    toplevel            boolean,
    CONSTRAINT pk_sample_kcache_n PRIMARY KEY (server_id, sample_id, datid, userid, queryid, toplevel),
    CONSTRAINT fk_kcache_stmt_list FOREIGN KEY (server_id,queryid_md5)
      REFERENCES stmt_list (server_id,queryid_md5)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_kcache_st FOREIGN KEY (server_id, sample_id, datid, userid, queryid, toplevel)
      REFERENCES sample_statements(server_id, sample_id, datid, userid, queryid, toplevel) ON DELETE CASCADE
);
CREATE INDEX ix_sample_kcache_sl ON sample_kcache(server_id,queryid_md5);

COMMENT ON TABLE sample_kcache IS 'Sample sample_kcache statistics table (fields from pg_stat_kcache)';

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

CREATE TABLE sample_kcache_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    plan_user_time           double precision, --  User CPU time used
    plan_system_time         double precision, --  System CPU time used
    plan_minflts              bigint, -- Number of page reclaims (soft page faults)
    plan_majflts              bigint, -- Number of page faults (hard page faults)
    plan_nswaps              bigint, -- Number of swaps
    plan_reads               bigint, -- Number of bytes read by the filesystem layer
    --plan_reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
    plan_writes              bigint, -- Number of bytes written by the filesystem layer
    --plan_writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
    plan_msgsnds             bigint, -- Number of IPC messages sent
    plan_msgrcvs             bigint, -- Number of IPC messages received
    plan_nsignals            bigint, -- Number of signals received
    plan_nvcsws              bigint, -- Number of voluntary context switches
    plan_nivcsws             bigint,
    exec_user_time           double precision, --  User CPU time used
    exec_system_time         double precision, --  System CPU time used
    exec_minflts              bigint, -- Number of page reclaims (soft page faults)
    exec_majflts              bigint, -- Number of page faults (hard page faults)
    exec_nswaps              bigint, -- Number of swaps
    exec_reads               bigint, -- Number of bytes read by the filesystem layer
    --exec_reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
    exec_writes              bigint, -- Number of bytes written by the filesystem layer
    --exec_writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
    exec_msgsnds             bigint, -- Number of IPC messages sent
    exec_msgrcvs             bigint, -- Number of IPC messages received
    exec_nsignals            bigint, -- Number of signals received
    exec_nvcsws              bigint, -- Number of voluntary context switches
    exec_nivcsws             bigint,
    statements               bigint NOT NULL,
    CONSTRAINT pk_sample_kcache_total PRIMARY KEY (server_id, sample_id, datid),
    CONSTRAINT fk_kcache_t_st FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_kcache_total IS 'Aggregated stats for kcache, based on pg_stat_kcache';
/* ==== Function stats history ==== */

CREATE TABLE funcs_list(
    server_id       integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    datid           oid,
    funcid          oid,
    schemaname      name NOT NULL,
    funcname        name NOT NULL,
    funcargs        text NOT NULL,
    last_sample_id  integer,
    CONSTRAINT pk_funcs_list PRIMARY KEY (server_id, datid, funcid),
    CONSTRAINT fk_funcs_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
CREATE INDEX ix_funcs_list_samples ON funcs_list (server_id, last_sample_id);
COMMENT ON TABLE funcs_list IS 'Function names and schemas, captured in samples';

CREATE TABLE sample_stat_user_functions (
    server_id   integer,
    sample_id   integer,
    datid       oid,
    funcid      oid,
    calls       bigint,
    total_time  double precision,
    self_time   double precision,
    trg_fn      boolean,
    CONSTRAINT fk_user_functions_functions FOREIGN KEY (server_id, datid, funcid)
      REFERENCES funcs_list (server_id, datid, funcid)
      ON DELETE NO ACTION
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_user_functions_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database (server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_user_functions PRIMARY KEY (server_id, sample_id, datid, funcid)
);
CREATE INDEX ix_sample_stat_user_functions_fl ON sample_stat_user_functions(server_id, datid, funcid);

COMMENT ON TABLE sample_stat_user_functions IS 'Stats increments for user functions in all databases by samples';

CREATE VIEW v_sample_stat_user_functions AS
    SELECT
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
    FROM sample_stat_user_functions JOIN funcs_list USING (server_id, datid, funcid);
COMMENT ON VIEW v_sample_stat_user_functions IS 'Reconstructed stats view with function names and schemas';

CREATE TABLE last_stat_user_functions (LIKE v_sample_stat_user_functions, in_sample boolean NOT NULL DEFAULT false)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_user_functions IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_user_func_total (
    server_id   integer,
    sample_id   integer,
    datid       oid,
    calls       bigint,
    total_time  double precision,
    trg_fn      boolean,
    CONSTRAINT fk_user_func_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database (server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_user_func_total PRIMARY KEY (server_id, sample_id, datid, trg_fn)
);
COMMENT ON TABLE sample_stat_user_func_total IS 'Total stats for user functions in all databases by samples';
/* === Data tables used in dump import process ==== */
CREATE TABLE import_queries_version_order (
  extension         text,
  version           text,
  parent_extension  text,
  parent_version    text,
  CONSTRAINT pk_import_queries_version_order PRIMARY KEY (extension, version),
  CONSTRAINT fk_import_queries_version_order FOREIGN KEY (parent_extension, parent_version)
    REFERENCES import_queries_version_order (extension,version)
);
COMMENT ON TABLE import_queries_version_order IS 'Version history used in import process';

CREATE TABLE import_queries (
  extension       text,
  from_version    text,
  exec_order      integer,
  relname         text,
  query           text NOT NULL,
  CONSTRAINT pk_import_queries PRIMARY KEY (extension, from_version, exec_order, relname),
  CONSTRAINT fk_import_queries_version FOREIGN KEY (extension, from_version)
    REFERENCES import_queries_version_order (extension,version)
);
COMMENT ON TABLE import_queries IS 'Queries, used in import process';
/* ==== Settings history table ==== */
CREATE TABLE sample_settings (
    server_id          integer,
    first_seen         timestamp (0) with time zone,
    setting_scope      smallint, -- Scope of setting. Currently may be 1 for pg_settings and 2 for other adm functions (like version)
    name               text,
    setting            text,
    reset_val          text,
    boot_val           text,
    unit               text,
    sourcefile          text,
    sourceline         integer,
    pending_restart    boolean,
    CONSTRAINT pk_sample_settings PRIMARY KEY (server_id, setting_scope, name, first_seen),
    CONSTRAINT fk_sample_settings_servers FOREIGN KEY (server_id)
      REFERENCES servers(server_id) ON DELETE CASCADE
);
-- Unique index on system_identifier to ensure there is no versions
-- as they are affecting export/import functionality
CREATE UNIQUE INDEX uk_sample_settings_sysid ON
  sample_settings (server_id,name) WHERE name='system_identifier';

COMMENT ON TABLE sample_settings IS 'pg_settings values changes detected at time of sample';

CREATE VIEW v_sample_settings AS
  SELECT
    server_id,
    sample_id,
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
  FROM samples s
    JOIN sample_settings ss USING (server_id)
    JOIN LATERAL
      (SELECT server_id, name, max(first_seen) as first_seen
        FROM sample_settings WHERE server_id = s.server_id AND first_seen <= s.sample_time
        GROUP BY server_id, name) lst
      USING (server_id, name, first_seen)
;
COMMENT ON VIEW v_sample_settings IS 'Provides postgres settings for samples';
/* ==== Sample taking time tracking storage ==== */
CREATE TABLE sample_timings (
    server_id   integer NOT NULL,
    sample_id   integer NOT NULL,
    event       text,
    time_spent  interval MINUTE TO SECOND (2),
    CONSTRAINT pk_sample_timings PRIMARY KEY (server_id, sample_id, event),
    CONSTRAINT fk_sample_timings_sample FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
);
COMMENT ON TABLE sample_timings IS 'Sample taking time statistics';

CREATE VIEW v_sample_timings AS
SELECT
  srv.server_name,
  smp.sample_id,
  smp.sample_time,
  tm.event as sampling_event,
  tm.time_spent
FROM
  sample_timings tm
  JOIN servers srv USING (server_id)
  JOIN samples smp USING (server_id, sample_id);
COMMENT ON VIEW v_sample_timings IS 'Sample taking time statistics with server names and sample times';
CREATE TABLE report_static (
  static_name     text,
  static_text     text,
  CONSTRAINT pk_report_headers PRIMARY KEY (static_name)
);

CREATE TABLE report (
  report_id           integer,
  report_name         text,
  report_description  text,
  template            text,
  CONSTRAINT pk_report PRIMARY KEY (report_id),
  CONSTRAINT fk_report_template FOREIGN KEY (template)
    REFERENCES report_static(static_name)
    ON UPDATE CASCADE
);

CREATE TABLE report_struct (
  report_id       integer,
  sect_id         text,
  parent_sect_id  text,
  s_ord           integer,
  toc_cap         text,
  tbl_cap         text,
  feature         text,
  function_name   text,
  href            text,
  content         text DEFAULT NULL,
  CONSTRAINT pk_report_struct PRIMARY KEY (report_id, sect_id),
  CONSTRAINT fk_report_struct_report FOREIGN KEY (report_id)
    REFERENCES report(report_id) ON UPDATE CASCADE,
  CONSTRAINT fk_report_struct_tree FOREIGN KEY (report_id, parent_sect_id)
    REFERENCES report_struct(report_id, sect_id) ON UPDATE CASCADE
);
CREATE INDEX ix_fk_report_struct_tree ON report_struct(report_id, parent_sect_id);
/* ==== Version history table data ==== */
INSERT INTO import_queries_version_order VALUES
('pg_profile','0.3.1',NULL,NULL),
('pg_profile','0.3.2','pg_profile','0.3.1'),
('pg_profile','0.3.3','pg_profile','0.3.2'),
('pg_profile','0.3.4','pg_profile','0.3.3'),
('pg_profile','0.3.5','pg_profile','0.3.4'),
('pg_profile','0.3.6','pg_profile','0.3.5'),
('pg_profile','3.8','pg_profile','0.3.6'),
('pg_profile','3.9','pg_profile','3.8'),
('pg_profile','4.0','pg_profile','3.9'),
('pg_profile','4.1','pg_profile','4.0')
;

/* ==== Data importing queries ==== */

INSERT INTO import_queries VALUES
('pg_profile','0.3.1', 1,'samples',
  'INSERT INTO samples (server_id,sample_id,sample_time) '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.sample_time '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id   integer, '
        'sample_id   integer, '
        'sample_time timestamp (0) with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN samples ld ON (ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_settings',
  'INSERT INTO sample_settings (server_id,first_seen,setting_scope,name,setting,'
    'reset_val,boot_val,unit,sourcefile,sourceline,pending_restart)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.first_seen, '
    'dt.setting_scope, '
    'dt.name, '
    'dt.setting, '
    'dt.reset_val, '
    'dt.boot_val, '
    'dt.unit, '
    'dt.sourcefile, '
    'dt.sourceline, '
    'dt.pending_restart '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id        integer, '
        'first_seen       timestamp(0) with time zone, '
        'setting_scope    smallint, '
        'name             text, '
        'setting          text, '
        'reset_val        text, '
        'boot_val         text, '
        'unit             text, '
        'sourcefile       text, '
        'sourceline       integer, '
        'pending_restart  boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_settings ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.name = dt.name AND ld.first_seen = dt.first_seen) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'baselines',
  'INSERT INTO baselines (server_id,bl_id,bl_name,keep_until)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.bl_id, '
    'dt.bl_name, '
    'dt.keep_until '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id    integer, '
        'bl_id        integer, '
        'bl_name      character varying(25), '
        'keep_until   timestamp (0) with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN baselines ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.bl_id = dt.bl_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.2', 1,'stmt_list',
  'INSERT INTO stmt_list (server_id,queryid_md5,query)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.queryid_md5, '
    'dt.query '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id    integer, '
        'queryid_md5  character(32), '
        'query        text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN stmt_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.queryid_md5 = dt.queryid_md5) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'tablespaces_list',
  'INSERT INTO tablespaces_list (server_id,tablespaceid,tablespacename,tablespacepath)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.tablespaceid, '
    'dt.tablespacename, '
    'dt.tablespacepath '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'tablespaceid   oid, '
        'tablespacename name, '
        'tablespacepath text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN tablespaces_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'tables_list',
  'INSERT INTO tables_list (server_id,last_sample_id,datid,relid,relkind,'
    'reltoastrelid,schemaname,relname)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.relkind, '
    'dt.reltoastrelid, '
    'dt.schemaname, '
    'dt.relname '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'datid          oid, '
        'relid          oid, '
        'relkind        character(1), '
        'reltoastrelid  oid, '
        'schemaname     name, '
        'relname        name '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN tables_list ld ON '
      '(ld.server_id, ld.datid, ld.relid, ld.last_sample_id, ld.schemaname, ld.relname) IS NOT DISTINCT FROM '
      '(srv_map.local_srv_id, dt.datid, dt.relid, dt.last_sample_id, dt.schemaname, dt.relname) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
  'ON CONFLICT ON CONSTRAINT pk_tables_list DO '
  'UPDATE SET (last_sample_id, schemaname, relname) = '
    '(EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.relname)'
),
('pg_profile','0.3.1', 1,'indexes_list',
  'INSERT INTO indexes_list (server_id,last_sample_id,datid,indexrelid,relid,'
    'schemaname,indexrelname)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.datid, '
    'dt.indexrelid, '
    'dt.relid, '
    'dt.schemaname, '
    'dt.indexrelname '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'datid          oid, '
        'indexrelid     oid, '
        'relid          oid, '
        'schemaname     name, '
        'indexrelname   name '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN indexes_list ld ON '
      '(ld.server_id, ld.datid, ld.indexrelid, ld.last_sample_id, ld.schemaname, ld.indexrelname) IS NOT DISTINCT FROM '
      '(srv_map.local_srv_id, dt.datid, dt.indexrelid, dt.last_sample_id, dt.schemaname, dt.indexrelname)'
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
  'ON CONFLICT ON CONSTRAINT pk_indexes_list DO '
  'UPDATE SET (last_sample_id, schemaname, indexrelname) = '
    ' (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.indexrelname)'
),
('pg_profile','0.3.1', 1,'funcs_list',
  'INSERT INTO funcs_list (server_id,last_sample_id,datid,funcid,schemaname,'
    'funcname,funcargs)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.datid, '
    'dt.funcid, '
    'dt.schemaname, '
    'dt.funcname, '
    'dt.funcargs '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'datid          oid, '
        'funcid         oid, '
        'schemaname     name, '
        'funcname       name, '
        'funcargs       text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN funcs_list ld ON '
      '(ld.server_id, ld.datid, ld.funcid, ld.last_sample_id, ld.schemaname, ld.funcname, ld.funcargs) IS NOT DISTINCT FROM '
      '(srv_map.local_srv_id, dt.datid, dt.funcid, dt.last_sample_id, dt.schemaname, dt.funcname, dt.funcargs) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
  'ON CONFLICT ON CONSTRAINT pk_funcs_list DO '
  'UPDATE SET (last_sample_id, schemaname, funcname, funcargs) = '
    '(EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.funcname, EXCLUDED.funcargs) '
),
('pg_profile','0.3.1', 1,'sample_timings',
  'INSERT INTO sample_timings (server_id,sample_id,event,time_spent)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.event, '
    'dt.time_spent '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'event          text, '
        'time_spent     interval minute to second(2) '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_timings ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.event = dt.event) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'bl_samples',
  'INSERT INTO bl_samples (server_id,sample_id,bl_id)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.bl_id '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'bl_id          integer '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN bl_samples ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.bl_id = dt.bl_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_stat_database',
  'INSERT INTO sample_stat_database (server_id,sample_id,datid,datname,'
    'xact_commit,xact_rollback,blks_read,blks_hit,tup_returned,tup_fetched,'
    'tup_inserted,tup_updated,tup_deleted,conflicts,temp_files,temp_bytes,'
    'deadlocks,checksum_failures,checksum_last_failure,blk_read_time,'
    'blk_write_time,stats_reset,datsize,'
    'datsize_delta,datistemplate,session_time,active_time,'
    'idle_in_transaction_time,sessions,sessions_abandoned,sessions_fatal,'
    'sessions_killed)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.datname, '
    'dt.xact_commit, '
    'dt.xact_rollback, '
    'dt.blks_read, '
    'dt.blks_hit, '
    'dt.tup_returned, '
    'dt.tup_fetched, '
    'dt.tup_inserted, '
    'dt.tup_updated, '
    'dt.tup_deleted, '
    'dt.conflicts, '
    'dt.temp_files, '
    'dt.temp_bytes, '
    'dt.deadlocks, '
    'dt.checksum_failures, '
    'dt.checksum_last_failure, '
    'dt.blk_read_time, '
    'dt.blk_write_time, '
    'dt.stats_reset, '
    'dt.datsize, '
    'dt.datsize_delta, '
    'dt.datistemplate, '
    'dt.session_time, '
    'dt.active_time, '
    'dt.idle_in_transaction_time, '
    'dt.sessions, '
    'dt.sessions_abandoned, '
    'dt.sessions_fatal, '
    'dt.sessions_killed '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id       integer, '
        'sample_id       integer, '
        'datid           oid, '
        'datname         name, '
        'xact_commit     bigint, '
        'xact_rollback   bigint, '
        'blks_read       bigint, '
        'blks_hit        bigint, '
        'tup_returned    bigint, '
        'tup_fetched     bigint, '
        'tup_inserted    bigint, '
        'tup_updated     bigint, '
        'tup_deleted     bigint, '
        'conflicts       bigint, '
        'temp_files      bigint, '
        'temp_bytes      bigint, '
        'deadlocks       bigint, '
        'blk_read_time   double precision, '
        'blk_write_time  double precision, '
        'stats_reset     timestamp with time zone, '
        'datsize         bigint, '
        'datsize_delta   bigint, '
        'datistemplate   boolean, '
        'session_time    double precision, '
        'active_time     double precision, '
        'idle_in_transaction_time  double precision, '
        'sessions        bigint, '
        'sessions_abandoned  bigint, '
        'sessions_fatal      bigint, '
        'sessions_killed     bigint, '
        'checksum_failures   bigint, '
        'checksum_last_failure timestamp with time zone'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_database ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'last_stat_database',
  'INSERT INTO last_stat_database (server_id,sample_id,datid,datname,xact_commit,'
    'xact_rollback,blks_read,blks_hit,tup_returned,tup_fetched,tup_inserted,'
    'tup_updated,tup_deleted,conflicts,temp_files,temp_bytes,deadlocks,'
    'checksum_failures,checksum_last_failure,'
    'blk_read_time,blk_write_time,stats_reset,datsize,datsize_delta,datistemplate,'
    'session_time,active_time,'
    'idle_in_transaction_time,sessions,sessions_abandoned,sessions_fatal,'
    'sessions_killed)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.datname, '
    'dt.xact_commit, '
    'dt.xact_rollback, '
    'dt.blks_read, '
    'dt.blks_hit, '
    'dt.tup_returned, '
    'dt.tup_fetched, '
    'dt.tup_inserted, '
    'dt.tup_updated, '
    'dt.tup_deleted, '
    'dt.conflicts, '
    'dt.temp_files, '
    'dt.temp_bytes, '
    'dt.deadlocks, '
    'dt.checksum_failures, '
    'dt.checksum_last_failure, '
    'dt.blk_read_time, '
    'dt.blk_write_time, '
    'dt.stats_reset, '
    'dt.datsize, '
    'dt.datsize_delta, '
    'dt.datistemplate, '
    'dt.session_time, '
    'dt.active_time, '
    'dt.idle_in_transaction_time, '
    'dt.sessions, '
    'dt.sessions_abandoned, '
    'dt.sessions_fatal, '
    'dt.sessions_killed '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id       integer, '
        'sample_id       integer, '
        'datid           oid, '
        'datname         name, '
        'xact_commit     bigint, '
        'xact_rollback   bigint, '
        'blks_read       bigint, '
        'blks_hit        bigint, '
        'tup_returned    bigint, '
        'tup_fetched     bigint, '
        'tup_inserted    bigint, '
        'tup_updated     bigint, '
        'tup_deleted     bigint, '
        'conflicts       bigint, '
        'temp_files      bigint, '
        'temp_bytes      bigint, '
        'deadlocks       bigint, '
        'blk_read_time   double precision, '
        'blk_write_time  double precision, '
        'stats_reset     timestamp with time zone, '
        'datsize         bigint, '
        'datsize_delta   bigint, '
        'datistemplate   boolean, '
        'session_time    double precision, '
        'active_time     double precision, '
        'idle_in_transaction_time  double precision, '
        'sessions        bigint, '
        'sessions_abandoned  bigint, '
        'sessions_fatal      bigint, '
        'sessions_killed     bigint, '
        'checksum_failures   bigint, '
        'checksum_last_failure timestamp with time zone'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_database ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.2', 1,'sample_statements',
  'INSERT INTO roles_list (server_id,userid,username'
    ')'
  'SELECT DISTINCT '
    'srv_map.local_srv_id, '
    'dt.userid, '
    '''_unknown_'' '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'userid               oid '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN roles_list ld ON '
      '(ld.server_id = srv_map.local_srv_id '
      'AND ld.userid = dt.userid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.2', 2,'sample_statements',
  'INSERT INTO sample_statements (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,'
    'stddev_plan_time,calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,'
    'stddev_exec_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,'
    'shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,'
    'local_blks_written,temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,'
    'wal_records,wal_fpi,wal_bytes,toplevel'
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
    'coalesce(dt.toplevel, true) '
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
        'toplevel             boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_statements ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, coalesce(dt.toplevel, true)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile', '0.3.2', 3, 'sample_statements',
  'UPDATE stmt_list sl SET last_sample_id = qid_smp.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, queryid_md5 '
    'FROM sample_statements '
    'GROUP BY server_id, queryid_md5'
    ') qid_smp '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (sl.server_id, sl.queryid_md5) = (qid_smp.server_id, qid_smp.queryid_md5) '
    'AND sl.last_sample_id IS NULL '
    'AND qid_smp.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.2', 4, 'sample_statements',
  'UPDATE roles_list rl SET last_sample_id = r_smp.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, userid '
    'FROM sample_statements '
    'GROUP BY server_id, userid'
    ') r_smp '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (rl.server_id, rl.userid) = (r_smp.server_id, r_smp.userid) '
    'AND rl.last_sample_id IS NULL '
    'AND r_smp.last_sample_id != ms.max_server_id'
),
('pg_profile','0.3.1', 1,'sample_stat_tablespaces',
  'INSERT INTO sample_stat_tablespaces (server_id,sample_id,tablespaceid,size,size_delta)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.tablespaceid, '
    'dt.size, '
    'dt.size_delta '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id     integer, '
        'sample_id     integer, '
        'tablespaceid  oid, '
        'size          bigint, '
        'size_delta    bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_tablespaces ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile', '0.3.1', 2, 'sample_stat_tablespaces',
  'UPDATE tablespaces_list tl SET last_sample_id = tsl.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, tablespaceid '
    'FROM sample_stat_tablespaces '
    'GROUP BY server_id, tablespaceid'
    ') tsl '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (tl.server_id, tl.tablespaceid) = (tsl.server_id, tsl.tablespaceid) '
    'AND tl.last_sample_id IS NULL '
    'AND tsl.last_sample_id != ms.max_server_id'
),
('pg_profile','0.3.1', 1,'last_stat_tablespaces',
  'INSERT INTO last_stat_tablespaces (server_id,sample_id,tablespaceid,tablespacename,'
    'tablespacepath,size,size_delta)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.tablespaceid, '
    'dt.tablespacename, '
    'dt.tablespacepath, '
    'dt.size, '
    'dt.size_delta '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id       integer, '
        'sample_id       integer, '
        'tablespaceid    oid, '
        'tablespacename  name, '
        'tablespacepath  text, '
        'size            bigint, '
        'size_delta      bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_tablespaces ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_stat_tables',
  'INSERT INTO sample_stat_tables (server_id,sample_id,datid,relid,tablespaceid,seq_scan,'
    'seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,n_tup_hot_upd,'
    'n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,last_vacuum,'
    'last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,autovacuum_count,'
    'analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,idx_blks_read,'
    'idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,tidx_blks_hit,'
    'relsize,relsize_diff,relpages_bytes,relpages_bytes_diff)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.tablespaceid, '
    'dt.seq_scan, '
    'dt.seq_tup_read, '
    'dt.idx_scan, '
    'dt.idx_tup_fetch, '
    'dt.n_tup_ins, '
    'dt.n_tup_upd, '
    'dt.n_tup_del, '
    'dt.n_tup_hot_upd, '
    'dt.n_live_tup, '
    'dt.n_dead_tup, '
    'dt.n_mod_since_analyze, '
    'dt.n_ins_since_vacuum, '
    'dt.last_vacuum, '
    'dt.last_autovacuum, '
    'dt.last_analyze, '
    'dt.last_autoanalyze, '
    'dt.vacuum_count, '
    'dt.autovacuum_count, '
    'dt.analyze_count, '
    'dt.autoanalyze_count, '
    'dt.heap_blks_read, '
    'dt.heap_blks_hit, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.toast_blks_read, '
    'dt.toast_blks_hit, '
    'dt.tidx_blks_read, '
    'dt.tidx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'datid                oid, '
        'relid                oid, '
        'tablespaceid         oid, '
        'seq_scan             bigint, '
        'seq_tup_read         bigint, '
        'idx_scan             bigint, '
        'idx_tup_fetch        bigint, '
        'n_tup_ins            bigint, '
        'n_tup_upd            bigint, '
        'n_tup_del            bigint, '
        'n_tup_hot_upd        bigint, '
        'n_live_tup           bigint, '
        'n_dead_tup           bigint, '
        'n_mod_since_analyze  bigint, '
        'n_ins_since_vacuum   bigint, '
        'last_vacuum          timestamp with time zone, '
        'last_autovacuum      timestamp with time zone, '
        'last_analyze         timestamp with time zone, '
        'last_autoanalyze     timestamp with time zone, '
        'vacuum_count         bigint, '
        'autovacuum_count     bigint, '
        'analyze_count        bigint, '
        'autoanalyze_count    bigint, '
        'heap_blks_read       bigint, '
        'heap_blks_hit        bigint, '
        'idx_blks_read        bigint, '
        'idx_blks_hit         bigint, '
        'toast_blks_read      bigint, '
        'toast_blks_hit       bigint, '
        'tidx_blks_read       bigint, '
        'tidx_blks_hit        bigint, '
        'relsize              bigint, '
        'relsize_diff         bigint, '
        'relpages_bytes       bigint, '
        'relpages_bytes_diff  bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile', '0.3.1', 2, 'sample_stat_tables',
  'UPDATE tables_list tl SET last_sample_id = isl.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, datid, relid '
    'FROM sample_stat_tables '
    'GROUP BY server_id, datid, relid'
    ') isl '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (tl.server_id, tl.datid, tl.relid) = (isl.server_id, isl.datid, isl.relid) '
    'AND tl.last_sample_id IS NULL '
    'AND isl.last_sample_id != ms.max_server_id'
),
('pg_profile','0.3.1', 1,'sample_stat_indexes',
  'INSERT INTO sample_stat_indexes (server_id,sample_id,datid,indexrelid,tablespaceid,'
    'idx_scan,idx_tup_read,idx_tup_fetch,idx_blks_read,idx_blks_hit,relsize,'
    'relsize_diff,indisunique,relpages_bytes,relpages_bytes_diff)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.indexrelid, '
    'dt.tablespaceid, '
    'dt.idx_scan, '
    'dt.idx_tup_read, '
    'dt.idx_tup_fetch, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.indisunique, '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'datid          oid, '
        'indexrelid     oid, '
        'tablespaceid   oid, '
        'idx_scan       bigint, '
        'idx_tup_read   bigint, '
        'idx_tup_fetch  bigint, '
        'idx_blks_read  bigint, '
        'idx_blks_hit   bigint, '
        'relsize        bigint, '
        'relsize_diff   bigint, '
        'indisunique    boolean, '
        'relpages_bytes bigint, '
        'relpages_bytes_diff bigint'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile', '0.3.1', 2, 'sample_stat_indexes',
  'UPDATE indexes_list il SET last_sample_id = isl.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, datid, indexrelid '
    'FROM sample_stat_indexes '
    'GROUP BY server_id, datid, indexrelid'
    ') isl '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (il.server_id, il.datid, il.indexrelid) = (isl.server_id, isl.datid, isl.indexrelid) '
    'AND il.last_sample_id IS NULL '
    'AND isl.last_sample_id != ms.max_server_id'
),
('pg_profile','0.3.1', 1,'sample_stat_user_functions',
  'INSERT INTO sample_stat_user_functions (server_id,sample_id,datid,funcid,'
    'calls,total_time,self_time,trg_fn)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.funcid, '
    'dt.calls, '
    'dt.total_time, '
    'dt.self_time, '
    'dt.trg_fn '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id   integer, '
        'sample_id   integer, '
        'datid       oid, '
        'funcid      oid, '
        'calls       bigint, '
        'total_time  double precision, '
        'self_time   double precision, '
        'trg_fn      boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_user_functions ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.funcid = dt.funcid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile', '0.3.1', 2, 'sample_stat_user_functions',
  'UPDATE funcs_list fl SET last_sample_id = isl.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, datid, funcid '
    'FROM sample_stat_user_functions '
    'GROUP BY server_id, datid, funcid'
    ') isl '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (fl.server_id, fl.datid, fl.funcid) = (isl.server_id, isl.datid, isl.funcid) '
    'AND fl.last_sample_id IS NULL '
    'AND isl.last_sample_id != ms.max_server_id'
),
('pg_profile','0.3.1', 1,'sample_stat_cluster',
  'INSERT INTO sample_stat_cluster (server_id,sample_id,checkpoints_timed,'
    'checkpoints_req,checkpoint_write_time,checkpoint_sync_time,buffers_checkpoint,'
    'buffers_clean,maxwritten_clean,buffers_backend,buffers_backend_fsync,'
    'buffers_alloc,stats_reset,wal_size)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.checkpoints_timed, '
    'dt.checkpoints_req, '
    'dt.checkpoint_write_time, '
    'dt.checkpoint_sync_time, '
    'dt.buffers_checkpoint, '
    'dt.buffers_clean, '
    'dt.maxwritten_clean, '
    'dt.buffers_backend, '
    'dt.buffers_backend_fsync, '
    'dt.buffers_alloc, '
    'dt.stats_reset, '
    'dt.wal_size '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id              integer, '
        'sample_id              integer, '
        'checkpoints_timed      bigint, '
        'checkpoints_req        bigint, '
        'checkpoint_write_time  double precision, '
        'checkpoint_sync_time   double precision, '
        'buffers_checkpoint     bigint, '
        'buffers_clean          bigint, '
        'maxwritten_clean       bigint, '
        'buffers_backend        bigint, '
        'buffers_backend_fsync  bigint, '
        'buffers_alloc          bigint, '
        'stats_reset            timestamp with time zone, '
        'wal_size               bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_cluster ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'last_stat_cluster',
  'INSERT INTO last_stat_cluster (server_id,sample_id,checkpoints_timed,'
    'checkpoints_req,checkpoint_write_time,checkpoint_sync_time,'
    'buffers_checkpoint,buffers_clean,maxwritten_clean,buffers_backend,'
    'buffers_backend_fsync,buffers_alloc,stats_reset,wal_size)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.checkpoints_timed, '
    'dt.checkpoints_req, '
    'dt.checkpoint_write_time, '
    'dt.checkpoint_sync_time, '
    'dt.buffers_checkpoint, '
    'dt.buffers_clean, '
    'dt.maxwritten_clean, '
    'dt.buffers_backend, '
    'dt.buffers_backend_fsync, '
    'dt.buffers_alloc, '
    'dt.stats_reset, '
    'dt.wal_size '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id              integer, '
        'sample_id              integer, '
        'checkpoints_timed      bigint, '
        'checkpoints_req        bigint, '
        'checkpoint_write_time  double precision, '
        'checkpoint_sync_time   double precision, '
        'buffers_checkpoint     bigint, '
        'buffers_clean          bigint, '
        'maxwritten_clean       bigint, '
        'buffers_backend        bigint, '
        'buffers_backend_fsync  bigint, '
        'buffers_alloc          bigint, '
        'stats_reset            timestamp with time zone, '
        'wal_size               bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_cluster ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_stat_archiver',
  'INSERT INTO sample_stat_archiver (server_id,sample_id,archived_count,last_archived_wal,'
    'last_archived_time,failed_count,last_failed_wal,last_failed_time,'
    'stats_reset)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.archived_count, '
    'dt.last_archived_wal, '
    'dt.last_archived_time, '
    'dt.failed_count, '
    'dt.last_failed_wal, '
    'dt.last_failed_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id           integer, '
        'sample_id           integer, '
        'archived_count      bigint, '
        'last_archived_wal   text, '
        'last_archived_time  timestamp with time zone, '
        'failed_count        bigint, '
        'last_failed_wal     text, '
        'last_failed_time    timestamp with time zone, '
        'stats_reset         timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_archiver ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'last_stat_archiver',
  'INSERT INTO last_stat_archiver (server_id,sample_id,archived_count,last_archived_wal,'
    'last_archived_time,failed_count,last_failed_wal,last_failed_time,stats_reset)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.archived_count, '
    'dt.last_archived_wal, '
    'dt.last_archived_time, '
    'dt.failed_count, '
    'dt.last_failed_wal, '
    'dt.last_failed_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id           integer, '
        'sample_id           integer, '
        'archived_count      bigint, '
        'last_archived_wal   text, '
        'last_archived_time  timestamp with time zone, '
        'failed_count        bigint, '
        'last_failed_wal     text, '
        'last_failed_time    timestamp with time zone, '
        'stats_reset         timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_archiver ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_statements_total',
  'INSERT INTO sample_statements_total (server_id,sample_id,datid,plans,total_plan_time,'
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
),
('pg_profile','0.3.2', 1,'sample_kcache',
  'INSERT INTO sample_kcache (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plan_user_time,plan_system_time,plan_minflts,plan_majflts,'
    'plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,'
    'plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,'
    'exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,'
    'exec_nsignals,exec_nvcsws,exec_nivcsws,toplevel'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.queryid, '
    'dt.queryid_md5, '
    'dt.plan_user_time, '
    'dt.plan_system_time, '
    'dt.plan_minflts, '
    'dt.plan_majflts, '
    'dt.plan_nswaps, '
    'dt.plan_reads, '
    'dt.plan_writes, '
    'dt.plan_msgsnds, '
    'dt.plan_msgrcvs, '
    'dt.plan_nsignals, '
    'dt.plan_nvcsws, '
    'dt.plan_nivcsws, '
    'dt.exec_user_time, '
    'dt.exec_system_time, '
    'dt.exec_minflts, '
    'dt.exec_majflts, '
    'dt.exec_nswaps, '
    'dt.exec_reads, '
    'dt.exec_writes, '
    'dt.exec_msgsnds, '
    'dt.exec_msgrcvs, '
    'dt.exec_nsignals, '
    'dt.exec_nvcsws, '
    'dt.exec_nivcsws, '
    'coalesce(dt.toplevel, true) '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id         integer, '
        'sample_id         integer, '
        'userid            oid, '
        'datid             oid, '
        'queryid           bigint, '
        'queryid_md5       character(32), '
        'plan_user_time    double precision, '
        'plan_system_time  double precision, '
        'plan_minflts      bigint, '
        'plan_majflts      bigint, '
        'plan_nswaps       bigint, '
        'plan_reads        bigint, '
        'plan_writes       bigint, '
        'plan_msgsnds      bigint, '
        'plan_msgrcvs      bigint, '
        'plan_nsignals     bigint, '
        'plan_nvcsws       bigint, '
        'plan_nivcsws      bigint, '
        'exec_user_time    double precision, '
        'exec_system_time  double precision, '
        'exec_minflts      bigint, '
        'exec_majflts      bigint, '
        'exec_nswaps       bigint, '
        'exec_reads        bigint, '
        'exec_writes       bigint, '
        'exec_msgsnds      bigint, '
        'exec_msgrcvs      bigint, '
        'exec_nsignals     bigint, '
        'exec_nvcsws       bigint, '
        'exec_nivcsws      bigint, '
        'toplevel          boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_kcache ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, coalesce(dt.toplevel, true)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_kcache_total',
  'INSERT INTO sample_kcache_total (server_id,sample_id,datid,plan_user_time,'
    'plan_system_time,plan_minflts,plan_majflts,plan_nswaps,plan_reads,plan_writes,'
    'plan_msgsnds,plan_msgrcvs,plan_nsignals,plan_nvcsws,plan_nivcsws,'
    'exec_user_time,exec_system_time,exec_minflts,exec_majflts,exec_nswaps,'
    'exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,exec_nsignals,exec_nvcsws,'
    'exec_nivcsws,statements)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.plan_user_time, '
    'dt.plan_system_time, '
    'dt.plan_minflts, '
    'dt.plan_majflts, '
    'dt.plan_nswaps, '
    'dt.plan_reads, '
    'dt.plan_writes, '
    'dt.plan_msgsnds, '
    'dt.plan_msgrcvs, '
    'dt.plan_nsignals, '
    'dt.plan_nvcsws, '
    'dt.plan_nivcsws, '
    'dt.exec_user_time, '
    'dt.exec_system_time, '
    'dt.exec_minflts, '
    'dt.exec_majflts, '
    'dt.exec_nswaps, '
    'dt.exec_reads, '
    'dt.exec_writes, '
    'dt.exec_msgsnds, '
    'dt.exec_msgrcvs, '
    'dt.exec_nsignals, '
    'dt.exec_nvcsws, '
    'dt.exec_nivcsws, '
    'dt.statements '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id         integer, '
        'sample_id         integer, '
        'datid             oid, '
        'plan_user_time    double precision, '
        'plan_system_time  double precision, '
        'plan_minflts      bigint, '
        'plan_majflts      bigint, '
        'plan_nswaps       bigint, '
        'plan_reads        bigint, '
        'plan_writes       bigint, '
        'plan_msgsnds      bigint, '
        'plan_msgrcvs      bigint, '
        'plan_nsignals     bigint, '
        'plan_nvcsws       bigint, '
        'plan_nivcsws      bigint, '
        'exec_user_time    double precision, '
        'exec_system_time  double precision, '
        'exec_minflts      bigint, '
        'exec_majflts      bigint, '
        'exec_nswaps       bigint, '
        'exec_reads        bigint, '
        'exec_writes       bigint, '
        'exec_msgsnds      bigint, '
        'exec_msgrcvs      bigint, '
        'exec_nsignals     bigint, '
        'exec_nvcsws       bigint, '
        'exec_nivcsws      bigint, '
        'statements        bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_kcache_total ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'last_stat_tables',
  'INSERT INTO last_stat_tables (server_id,sample_id,datid,relid,schemaname,relname,'
    'seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,'
    'n_tup_hot_upd,n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,'
    'last_vacuum,last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,'
    'autovacuum_count,analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,'
    'idx_blks_read,idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,'
    'tidx_blks_hit,relsize,relsize_diff,tablespaceid,reltoastrelid,relkind,in_sample,'
    'relpages_bytes, relpages_bytes_diff)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.schemaname, '
    'dt.relname, '
    'dt.seq_scan, '
    'dt.seq_tup_read, '
    'dt.idx_scan, '
    'dt.idx_tup_fetch, '
    'dt.n_tup_ins, '
    'dt.n_tup_upd, '
    'dt.n_tup_del, '
    'dt.n_tup_hot_upd, '
    'dt.n_live_tup, '
    'dt.n_dead_tup, '
    'dt.n_mod_since_analyze, '
    'dt.n_ins_since_vacuum, '
    'dt.last_vacuum, '
    'dt.last_autovacuum, '
    'dt.last_analyze, '
    'dt.last_autoanalyze, '
    'dt.vacuum_count, '
    'dt.autovacuum_count, '
    'dt.analyze_count, '
    'dt.autoanalyze_count, '
    'dt.heap_blks_read, '
    'dt.heap_blks_hit, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.toast_blks_read, '
    'dt.toast_blks_hit, '
    'dt.tidx_blks_read, '
    'dt.tidx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.tablespaceid, '
    'dt.reltoastrelid, '
    'dt.relkind, '
    'COALESCE(dt.in_sample, false), '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'datid                oid, '
        'relid                oid, '
        'schemaname           name, '
        'relname              name, '
        'seq_scan             bigint, '
        'seq_tup_read         bigint, '
        'idx_scan             bigint, '
        'idx_tup_fetch        bigint, '
        'n_tup_ins            bigint, '
        'n_tup_upd            bigint, '
        'n_tup_del            bigint, '
        'n_tup_hot_upd        bigint, '
        'n_live_tup           bigint, '
        'n_dead_tup           bigint, '
        'n_mod_since_analyze  bigint, '
        'n_ins_since_vacuum   bigint, '
        'last_vacuum          timestamp with time zone, '
        'last_autovacuum      timestamp with time zone, '
        'last_analyze         timestamp with time zone, '
        'last_autoanalyze     timestamp with time zone, '
        'vacuum_count         bigint, '
        'autovacuum_count     bigint, '
        'analyze_count        bigint, '
        'autoanalyze_count    bigint, '
        'heap_blks_read       bigint, '
        'heap_blks_hit        bigint, '
        'idx_blks_read        bigint, '
        'idx_blks_hit         bigint, '
        'toast_blks_read      bigint, '
        'toast_blks_hit       bigint, '
        'tidx_blks_read       bigint, '
        'tidx_blks_hit        bigint, '
        'relsize              bigint, '
        'relsize_diff         bigint, '
        'tablespaceid         oid, '
        'reltoastrelid        oid, '
        'relkind              character(1), '
        'in_sample            boolean, '
        'relpages_bytes       bigint, '
        'relpages_bytes_diff  bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_stat_tables_total',
  'INSERT INTO sample_stat_tables_total (server_id,sample_id,datid,tablespaceid,relkind,'
    'seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,'
    'n_tup_hot_upd,vacuum_count,autovacuum_count,analyze_count,autoanalyze_count,'
    'heap_blks_read,heap_blks_hit,idx_blks_read,idx_blks_hit,toast_blks_read,'
    'toast_blks_hit,tidx_blks_read,tidx_blks_hit,relsize_diff)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.tablespaceid, '
    'dt.relkind, '
    'dt.seq_scan, '
    'dt.seq_tup_read, '
    'dt.idx_scan, '
    'dt.idx_tup_fetch, '
    'dt.n_tup_ins, '
    'dt.n_tup_upd, '
    'dt.n_tup_del, '
    'dt.n_tup_hot_upd, '
    'dt.vacuum_count, '
    'dt.autovacuum_count, '
    'dt.analyze_count, '
    'dt.autoanalyze_count, '
    'dt.heap_blks_read, '
    'dt.heap_blks_hit, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.toast_blks_read, '
    'dt.toast_blks_hit, '
    'dt.tidx_blks_read, '
    'dt.tidx_blks_hit, '
    'dt.relsize_diff '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id          integer, '
        'sample_id          integer, '
        'datid              oid, '
        'tablespaceid       oid, '
        'relkind            character(1), '
        'seq_scan           bigint, '
        'seq_tup_read       bigint, '
        'idx_scan           bigint, '
        'idx_tup_fetch      bigint, '
        'n_tup_ins          bigint, '
        'n_tup_upd          bigint, '
        'n_tup_del          bigint, '
        'n_tup_hot_upd      bigint, '
        'vacuum_count       bigint, '
        'autovacuum_count   bigint, '
        'analyze_count      bigint, '
        'autoanalyze_count  bigint, '
        'heap_blks_read     bigint, '
        'heap_blks_hit      bigint, '
        'idx_blks_read      bigint, '
        'idx_blks_hit       bigint, '
        'toast_blks_read    bigint, '
        'toast_blks_hit     bigint, '
        'tidx_blks_read     bigint, '
        'tidx_blks_hit      bigint, '
        'relsize_diff       bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_tables_total ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'last_stat_indexes',
  'INSERT INTO last_stat_indexes (server_id,sample_id,datid,relid,indexrelid,'
    'schemaname,relname,indexrelname,idx_scan,idx_tup_read,idx_tup_fetch,'
    'idx_blks_read,idx_blks_hit,relsize,relsize_diff,tablespaceid,indisunique,'
    'in_sample,relpages_bytes,relpages_bytes_diff)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.indexrelid, '
    'dt.schemaname, '
    'dt.relname, '
    'dt.indexrelname, '
    'dt.idx_scan, '
    'dt.idx_tup_read, '
    'dt.idx_tup_fetch, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.tablespaceid, '
    'dt.indisunique, '
    'COALESCE(dt.in_sample, false), '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'datid          oid, '
        'relid          oid, '
        'indexrelid     oid, '
        'schemaname     name, '
        'relname        name, '
        'indexrelname   name, '
        'idx_scan       bigint, '
        'idx_tup_read   bigint, '
        'idx_tup_fetch  bigint, '
        'idx_blks_read  bigint, '
        'idx_blks_hit   bigint, '
        'relsize        bigint, '
        'relsize_diff   bigint, '
        'tablespaceid   oid, '
        'indisunique    boolean, '
        'in_sample      boolean, '
        'relpages_bytes bigint, '
        'relpages_bytes_diff bigint'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id '
      'AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_stat_indexes_total',
  'INSERT INTO sample_stat_indexes_total (server_id,sample_id,datid,tablespaceid,idx_scan,'
    'idx_tup_read,idx_tup_fetch,idx_blks_read,idx_blks_hit,relsize_diff)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.tablespaceid, '
    'dt.idx_scan, '
    'dt.idx_tup_read, '
    'dt.idx_tup_fetch, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.relsize_diff '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'datid          oid, '
        'tablespaceid   oid, '
        'idx_scan       bigint, '
        'idx_tup_read   bigint, '
        'idx_tup_fetch  bigint, '
        'idx_blks_read  bigint, '
        'idx_blks_hit   bigint, '
        'relsize_diff   bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_indexes_total ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'last_stat_user_functions',
  'INSERT INTO last_stat_user_functions (server_id,sample_id,datid,funcid,schemaname,'
    'funcname,funcargs,calls,total_time,self_time,trg_fn,in_sample)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.funcid, '
    'dt.schemaname, '
    'dt.funcname, '
    'dt.funcargs, '
    'dt.calls, '
    'dt.total_time, '
    'dt.self_time, '
    'dt.trg_fn, '
    'COALESCE(dt.in_sample, false) '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id   integer, '
        'sample_id   integer, '
        'datid       oid, '
        'funcid      oid, '
        'schemaname  name, '
        'funcname    name, '
        'funcargs    text, '
        'calls       bigint, '
        'total_time  double precision, '
        'self_time   double precision, '
        'trg_fn      boolean, '
        'in_sample   boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_user_functions ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.funcid = dt.funcid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_stat_user_func_total',
  'INSERT INTO sample_stat_user_func_total (server_id,sample_id,datid,calls,'
    'total_time,trg_fn)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.calls, '
    'dt.total_time, '
    'dt.trg_fn '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id   integer, '
        'sample_id   integer, '
        'datid       oid, '
        'calls       bigint, '
        'total_time  double precision, '
        'trg_fn      boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_user_func_total ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id '
      'AND ld.datid = dt.datid AND ld.trg_fn = dt.trg_fn) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_stat_tables_failures',
  'SELECT ''%1$s'' as imp WHERE -1 = $1'),
('pg_profile','0.3.1', 1,'sample_stat_indexes_failures',
  'SELECT ''%1$s'' as imp WHERE -1 = $1');
 /*
  * Support import from pg_profile 0.3.1
  */
INSERT INTO import_queries VALUES
-- queryid_md5 mapping temporary table
('pg_profile','0.3.1', 1,'stmt_list',
  'CREATE TEMPORARY TABLE queryid_map('
    'server_id,'
    'queryid_md5_old,'
    'queryid_md5_new'
  ') '
  'ON COMMIT DROP '
  'AS SELECT DISTINCT '
    'srv_map.local_srv_id, '
    'dt.queryid_md5, '
    'md5(dt.query) '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id    integer, '
        'queryid_md5  character(10), '
        'query        text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN stmt_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.queryid_md5 = md5(dt.query)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
-- Actual statements list load
('pg_profile','0.3.1', 2,'stmt_list',
  'INSERT INTO stmt_list (server_id,queryid_md5,query)'
  'SELECT DISTINCT '
    'srv_map.local_srv_id, '
    'md5(dt.query), '
    'dt.query '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id    integer, '
        'query        text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN stmt_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.queryid_md5 = md5(dt.query)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_statements',
  'INSERT INTO roles_list (server_id,userid,username'
    ')'
  'SELECT DISTINCT '
    'srv_map.local_srv_id, '
    'dt.userid, '
    '''_unknown_'' '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'userid               oid '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN roles_list ld ON '
      '(ld.server_id = srv_map.local_srv_id '
      'AND ld.userid = dt.userid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 2,'sample_statements',
  'INSERT INTO sample_statements (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,stddev_plan_time,'
    'calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,stddev_exec_time,'
    'rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,shared_blks_written,'
    'local_blks_hit,local_blks_read,local_blks_dirtied,local_blks_written,'
    'temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,wal_records,'
    'wal_fpi,wal_bytes,toplevel'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.queryid, '
    'q_map.queryid_md5_new, '
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
    'COALESCE(dt.toplevel, true) AS toplevel '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'userid               oid, '
        'datid                oid, '
        'queryid              bigint, '
        'queryid_md5          character(10), '
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
        'toplevel             boolean'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'JOIN queryid_map q_map ON (srv_map.local_srv_id, dt.queryid_md5) = (q_map.server_id, q_map.queryid_md5_old) '
    'LEFT OUTER JOIN sample_statements ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, '
      'COALESCE(dt.toplevel, true)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile', '0.3.1', 3, 'sample_statements',
  'UPDATE stmt_list sl SET last_sample_id = qid_smp.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, queryid_md5 '
    'FROM sample_statements '
    'GROUP BY server_id, queryid_md5'
    ') qid_smp '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (sl.server_id, sl.queryid_md5) = (qid_smp.server_id, qid_smp.queryid_md5) '
    'AND sl.last_sample_id IS NULL '
    'AND qid_smp.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.1', 4, 'sample_statements',
  'UPDATE roles_list rl SET last_sample_id = r_smp.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, userid '
    'FROM sample_statements '
    'GROUP BY server_id, userid'
    ') r_smp '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (rl.server_id, rl.userid) = (r_smp.server_id, r_smp.userid) '
    'AND rl.last_sample_id IS NULL '
    'AND r_smp.last_sample_id != ms.max_server_id'
),
('pg_profile','0.3.1', 1,'sample_kcache',
  'INSERT INTO sample_kcache (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plan_user_time,plan_system_time,plan_minflts,plan_majflts,'
    'plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,'
    'plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,'
    'exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,'
    'exec_nsignals,exec_nvcsws,exec_nivcsws,toplevel'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.queryid, '
    'q_map.queryid_md5_new, '
    'dt.plan_user_time, '
    'dt.plan_system_time, '
    'dt.plan_minflts, '
    'dt.plan_majflts, '
    'dt.plan_nswaps, '
    'dt.plan_reads, '
    'dt.plan_writes, '
    'dt.plan_msgsnds, '
    'dt.plan_msgrcvs, '
    'dt.plan_nsignals, '
    'dt.plan_nvcsws, '
    'dt.plan_nivcsws, '
    'dt.exec_user_time, '
    'dt.exec_system_time, '
    'dt.exec_minflts, '
    'dt.exec_majflts, '
    'dt.exec_nswaps, '
    'dt.exec_reads, '
    'dt.exec_writes, '
    'dt.exec_msgsnds, '
    'dt.exec_msgrcvs, '
    'dt.exec_nsignals, '
    'dt.exec_nvcsws, '
    'dt.exec_nivcsws, '
    'COALESCE(dt.toplevel, true) AS toplevel '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id         integer, '
        'sample_id         integer, '
        'userid            oid, '
        'datid             oid, '
        'queryid           bigint, '
        'queryid_md5       character(10), '
        'plan_user_time    double precision, '
        'plan_system_time  double precision, '
        'plan_minflts      bigint, '
        'plan_majflts      bigint, '
        'plan_nswaps       bigint, '
        'plan_reads        bigint, '
        'plan_writes       bigint, '
        'plan_msgsnds      bigint, '
        'plan_msgrcvs      bigint, '
        'plan_nsignals     bigint, '
        'plan_nvcsws       bigint, '
        'plan_nivcsws      bigint, '
        'exec_user_time    double precision, '
        'exec_system_time  double precision, '
        'exec_minflts      bigint, '
        'exec_majflts      bigint, '
        'exec_nswaps       bigint, '
        'exec_reads        bigint, '
        'exec_writes       bigint, '
        'exec_msgsnds      bigint, '
        'exec_msgrcvs      bigint, '
        'exec_nsignals     bigint, '
        'exec_nvcsws       bigint, '
        'exec_nivcsws      bigint, '
        'toplevel          boolean'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'JOIN queryid_map q_map ON (srv_map.local_srv_id, dt.queryid_md5) = (q_map.server_id, q_map.queryid_md5_old) '
    'LEFT OUTER JOIN sample_kcache ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, '
      'COALECSE(dt.toplevel, true)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
)
;
/* ===== V0.3.4 ===== */
INSERT INTO import_queries VALUES
('pg_profile','0.3.4', 1,'sample_stat_wal',
  'INSERT INTO sample_stat_wal (server_id,sample_id,wal_records,'
    'wal_fpi,wal_bytes,wal_buffers_full,wal_write,wal_sync,'
    'wal_write_time,wal_sync_time,stats_reset)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.wal_records, '
    'dt.wal_fpi, '
    'dt.wal_bytes, '
    'dt.wal_buffers_full, '
    'dt.wal_write, '
    'dt.wal_sync, '
    'dt.wal_write_time, '
    'dt.wal_sync_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id              integer, '
        'sample_id              integer, '
        'wal_records         bigint, '
        'wal_fpi             bigint, '
        'wal_bytes           numeric, '
        'wal_buffers_full    bigint, '
        'wal_write           bigint, '
        'wal_sync            bigint, '
        'wal_write_time      double precision, '
        'wal_sync_time       double precision, '
        'stats_reset            timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_wal ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.4', 1,'last_stat_wal',
  'INSERT INTO last_stat_wal (server_id,sample_id,wal_records,'
    'wal_fpi,wal_bytes,wal_buffers_full,wal_write,wal_sync,'
    'wal_write_time,wal_sync_time,stats_reset)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.wal_records, '
    'dt.wal_fpi, '
    'dt.wal_bytes, '
    'dt.wal_buffers_full, '
    'dt.wal_write, '
    'dt.wal_sync, '
    'dt.wal_write_time, '
    'dt.wal_sync_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id              integer, '
        'sample_id              integer, '
        'wal_records         bigint, '
        'wal_fpi             bigint, '
        'wal_bytes           numeric, '
        'wal_buffers_full    bigint, '
        'wal_write           bigint, '
        'wal_sync            bigint, '
        'wal_write_time      double precision, '
        'wal_sync_time       double precision, '
        'stats_reset            timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_wal ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
);

 /*
  * Support import from pg_profile 0.3.5
  */
-- roles
INSERT INTO import_queries VALUES
('pg_profile','0.3.5', 1,'roles_list',
  'INSERT INTO roles_list (server_id,userid,username)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.userid, '
    'dt.username '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id  integer, '
        'userid     oid, '
        'username   name '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN roles_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.userid = dt.userid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
);

 /*
  * Support import from pg_profile 3.8
  */
-- wait sampling
INSERT INTO import_queries VALUES
('pg_profile','3.8', 1,'wait_sampling_total',
  'INSERT INTO wait_sampling_total (server_id,sample_id,sample_wevnt_id,'
  'event_type,event,tot_waited,stmt_waited)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.sample_wevnt_id, '
    'dt.event_type, '
    'dt.event, '
    'dt.tot_waited, '
    'dt.stmt_waited '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id           integer, '
        'sample_id           integer, '
        'sample_wevnt_id     integer, '
        'event_type          text, '
        'event               text, '
        'tot_waited          bigint, '
        'stmt_waited         bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN wait_sampling_total ld ON '
      '(ld.server_id, ld.sample_id, ld.sample_wevnt_id) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.sample_wevnt_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
);

 /*
  * Support import from pg_profile 3.9
  */
INSERT INTO import_queries VALUES
('pg_profile','3.9', 1,'stmt_list',
  'INSERT INTO stmt_list (server_id,last_sample_id,queryid_md5,query)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.queryid_md5, '
    'dt.query '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'queryid_md5    character(32), '
        'query          text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN stmt_list ld ON '
      '(ld.server_id, ld.queryid_md5, ld.last_sample_id) IS NOT DISTINCT FROM'
      '(srv_map.local_srv_id, dt.queryid_md5, dt.last_sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
  'ON CONFLICT ON CONSTRAINT pk_stmt_list '
  'DO UPDATE SET last_sample_id = EXCLUDED.last_sample_id'
),
('pg_profile','3.9', 1,'tablespaces_list',
  'INSERT INTO tablespaces_list (server_id,last_sample_id,tablespaceid,tablespacename,tablespacepath)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.tablespaceid, '
    'dt.tablespacename, '
    'dt.tablespacepath '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'tablespaceid   oid, '
        'tablespacename name, '
        'tablespacepath text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN tablespaces_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','3.9', 1,'roles_list',
  'INSERT INTO roles_list (server_id,last_sample_id,userid,username)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.userid, '
    'dt.username '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'userid         oid, '
        'username       name '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN roles_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.userid = dt.userid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
)
;

 /*
  * Support import from pg_profile 4.0
  */
INSERT INTO import_queries VALUES
('pg_profile','4.0', 1,'last_stat_kcache',
  'INSERT INTO last_stat_kcache (server_id,sample_id,userid,datid,toplevel,queryid,'
    'plan_user_time,plan_system_time,plan_minflts,plan_majflts,'
    'plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,'
    'plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,'
    'exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,'
    'exec_nsignals,exec_nvcsws,exec_nivcsws'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.toplevel, '
    'dt.queryid, '
    'dt.plan_user_time, '
    'dt.plan_system_time, '
    'dt.plan_minflts, '
    'dt.plan_majflts, '
    'dt.plan_nswaps, '
    'dt.plan_reads, '
    'dt.plan_writes, '
    'dt.plan_msgsnds, '
    'dt.plan_msgrcvs, '
    'dt.plan_nsignals, '
    'dt.plan_nvcsws, '
    'dt.plan_nivcsws, '
    'dt.exec_user_time, '
    'dt.exec_system_time, '
    'dt.exec_minflts, '
    'dt.exec_majflts, '
    'dt.exec_nswaps, '
    'dt.exec_reads, '
    'dt.exec_writes, '
    'dt.exec_msgsnds, '
    'dt.exec_msgrcvs, '
    'dt.exec_nsignals, '
    'dt.exec_nvcsws, '
    'dt.exec_nivcsws '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id         integer, '
        'sample_id         integer, '
        'userid            oid, '
        'datid             oid, '
        'toplevel          boolean, '
        'queryid           bigint, '
        'plan_user_time    double precision, '
        'plan_system_time  double precision, '
        'plan_minflts      bigint, '
        'plan_majflts      bigint, '
        'plan_nswaps       bigint, '
        'plan_reads        bigint, '
        'plan_writes       bigint, '
        'plan_msgsnds      bigint, '
        'plan_msgrcvs      bigint, '
        'plan_nsignals     bigint, '
        'plan_nvcsws       bigint, '
        'plan_nivcsws      bigint, '
        'exec_user_time    double precision, '
        'exec_system_time  double precision, '
        'exec_minflts      bigint, '
        'exec_majflts      bigint, '
        'exec_nswaps       bigint, '
        'exec_reads        bigint, '
        'exec_writes       bigint, '
        'exec_msgsnds      bigint, '
        'exec_msgrcvs      bigint, '
        'exec_nsignals     bigint, '
        'exec_nvcsws       bigint, '
        'exec_nivcsws      bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_kcache ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, dt.toplevel) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.0', 1,'sample_statements',
  'INSERT INTO sample_statements (server_id,sample_id,userid,datid,queryid,queryid_md5,'
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
),
('pg_profile','4.0', 1,'last_stat_statements',
  'INSERT INTO last_stat_statements (server_id,sample_id,userid,username,datid,queryid,queryid_md5,'
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
)
;
/* === Data of reports === */

INSERT INTO report_static(static_name, static_text)
VALUES
('css1',
  'table, th, td {border: 1px solid black; border-collapse: collapse; padding:4px;} '
  'table tr td.value, table tr td.mono {font-family: Monospace;} '
  'table tr td.value {text-align: right;} '
  'table p {margin: 0.2em;}'
  'table tr.parent td:not(.hdr) {background-color: #D8E8C2;} '
  'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} '
  'table.stat tr:nth-child(even), table.setlist tr:nth-child(even) {background-color: #eee;} '
  'table.stat tr:nth-child(odd), table.setlist tr:nth-child(odd) {background-color: #fff;} '
  'table tr:hover td:not(.hdr) {background-color:#d9ffcc} '
  'table th {color: black; background-color: #ffcc99;}'
  'table tr:target,td:target {border: solid; border-width: medium; border-color:limegreen;}'
  'table tr:target td:first-of-type, table td:target {font-weight: bold;}'
  '{static:css1_post}'
),
('version',
  '<p>pg_profile version {properties:pgprofile_version}</p>'),
('report',
  '<html><head><style>{static:css1}</style>'
  '<title>Postgres profile report ({properties:start1_id} -'
  ' {properties:end1_id})</title></head><body>'
  '<H1>Postgres profile report ({properties:start1_id} -'
  ' {properties:end1_id})</H1>'
  '{static:version}'
  '<p>Server name: <strong>{properties:server_name}</strong></p>'
  '{properties:server_description}'
  '<p>Report interval: <strong>{properties:report_start1} -'
  ' {properties:report_end1}</strong></p>'
  '{properties:description}'
  '<h2>Report sections</h2>'
  '{report:toc}{report:sect}</body></html>'),
('css2',
  'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} '
  'table .value, table .mono {font-family: Monospace;} '
  'table .value {text-align: right;} '
  'table p {margin: 0.2em;}'
  '.int1 td:not(.hdr), td.int1 {background-color: #FFEEEE;} '
  '.int2 td:not(.hdr), td.int2 {background-color: #EEEEFF;} '
  'table.diff tr.int2 td {border-top: hidden;} '
  'table.stat tr:nth-child(even), table.setlist tr:nth-child(even) {background-color: #eee;} '
  'table.stat tr:nth-child(odd), table.setlist tr:nth-child(odd) {background-color: #fff;} '
  'table tr:hover td:not(.hdr) {background-color:#d9ffcc} '
  'table th {color: black; background-color: #ffcc99;}'
  '.label {color: grey;}'
  'table tr:target,td:target {border: solid; border-width: medium; border-color: limegreen;}'
  'table tr:target td:first-of-type, table td:target {font-weight: bold;}'
  'table tr.parent td {background-color: #D8E8C2;} '
  'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} '
  '{static:css2_post}'
),
('diffreport',
  '<html><head><style>{static:css2}</style>'
  '<title>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</title></head><body>'
  '<H1>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</H1>'
  '{static:version}'
  '<p>Server name: <strong>{properties:server_name}</strong></p>'
  '{properties:server_description}'
  '<p>First interval (1): <strong>{properties:report_start1} -'
  ' {properties:report_end1}</strong></p>'
  '<p>Second interval (2): <strong>{properties:report_start2} -'
  ' {properties:report_end2}</strong></p>'
  '{properties:description}'
  '<h2>Report sections</h2>'
  '{report:toc}{report:sect}</body></html>')
;

INSERT INTO report(report_id, report_name, report_description, template)
VALUES
(1, 'report', 'Regular single interval report', 'report'),
(2, 'diffreport', 'Differential report on two intervals', 'diffreport')
;

-- Regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'stmt_cmt1', NULL, 100, NULL, NULL, NULL, 'check_stmt_cnt_first_htbl', NULL, '<p><strong>Warning!</strong></p>'
  '<p>This interval contains sample(s) with captured statements count more than 90% of <i>pg_stat_statements.max</i> parameter.</p>'
  '{func_output}'
  '<p> Consider increasing <i>pg_stat_statements.max</i> parameter.</p>'),
(1, 'srvstat', NULL, 200, 'Server statistics', 'Server statistics', NULL, NULL, 'cl_stat', NULL),
(1, 'sqlsthdr', NULL, 300, 'SQL query statistics', 'SQL query statistics', 'statstatements', NULL, 'sql_stat', NULL),
(1, 'objects', NULL, 400, 'Schema object statistics', 'Schema object statistics', NULL, NULL, 'schema_stat', NULL),
(1, 'funchdr', NULL, 500, 'User function statistics', 'User function statistics', 'function_stats', NULL, 'func_stat', NULL),
(1, 'vachdr', NULL, 600, 'Vacuum-related statistics', 'Vacuum-related statistics', NULL, NULL, 'vacuum_stats', NULL),
(1, 'setings', NULL, 700, 'Cluster settings during the report interval', 'Cluster settings during the report interval', NULL, 'settings_and_changes_htbl', 'pg_settings', NULL),
(1, 'stmt_warn', NULL, 800, NULL, 'Warning!', NULL, 'check_stmt_cnt_all_htbl', NULL, NULL)
;

-- Server section of regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'dbstat1', 'srvstat', 100, 'Database statistics', 'Database statistics', NULL, NULL, 'db_stat', NULL),
(1, 'dbstat2', 'srvstat', 200, NULL, NULL, NULL, 'dbstats_reset_htbl', NULL,
  '<p><b>Warning!</b> Database statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Statistics for listed databases and contained objects might be affected</p>'),
(1, 'dbstat3', 'srvstat', 300, NULL, NULL, NULL, 'dbstats_htbl', NULL, NULL),
(1, 'sesstat', 'srvstat', 400, 'Session statistics by database', 'Session statistics by database', 'sess_stats', 'dbstats_sessions_htbl', 'db_stat_sessions', NULL),
(1, 'stmtstat', 'srvstat', 500, 'Statement statistics by database', 'Statement statistics by database', 'statstatements', 'statements_stats_htbl', 'st_stat', NULL),
(1, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', 'dbagg_jit_stats_htbl', 'dbagg_jit_stat', NULL),
(1, 'div1', 'srvstat', 600, NULL, NULL, NULL, NULL, NULL, '<div>'),
(1, 'clusthdr', 'srvstat', 700, 'Cluster statistics', 'Cluster statistics', NULL, NULL, 'clu_stat',
  '<div style="display:inline-block; margin-right:2em;">{header}'),
(1, 'clustrst', 'srvstat', 800, NULL, NULL, NULL, 'cluster_stats_reset_htbl', NULL,
  '<p><b>Warning!</b> Cluster statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Cluster statistics might be affected</p>'),
(1, 'clust', 'srvstat', 900, NULL, NULL, NULL, 'cluster_stats_htbl', NULL, '{func_output}</div>'),
(1, 'walsthdr', 'srvstat', 1000, 'WAL statistics', 'WAL statistics', 'wal_stats', NULL, 'wal_stat',
  '<div style="display:inline-block; margin-right:2em;">{header}'),
(1, 'walstrst', 'srvstat', 1100, NULL, NULL, 'wal_stats', 'wal_stats_reset_htbl', NULL,
  '<p><b>Warning!</b> WAL statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>WAL statistics might be affected</p>'),
(1, 'walst', 'srvstat', 1200, NULL, NULL, 'wal_stats', 'wal_stats_htbl', NULL, '{func_output}</div>'),
(1, 'div2', 'srvstat', 1300, NULL, NULL, NULL, NULL, NULL, '</div>'),
(1, 'tbspst', 'srvstat', 1400, 'Tablespace statistics', 'Tablespace statistics', NULL, 'tablespaces_stats_htbl', 'tablespace_stat', NULL),
(1, 'wait_sampling_srvstats', 'srvstat', 1500, 'Wait sampling', 'Wait sampling', 'wait_sampling_tot', NULL, 'wait_sampling', NULL),
(1, 'wait_sampling_total', 'wait_sampling_srvstats', 100, 'Wait events types', 'Wait events types', 'wait_sampling_tot', 'wait_sampling_totals_htbl', 'wait_sampling_total', NULL),
(1, 'wait_sampling_statements', 'wait_sampling_srvstats', 200, 'Top wait events (statements)', 'Top wait events (statements)', 'wait_sampling_tot', 'top_wait_sampling_events_htbl', 'wt_smp_stmt', '<p>Top wait events detected in statements execution</p>'),
(1, 'wait_sampling_all', 'wait_sampling_srvstats', 300, 'Top wait events (All)', 'Top wait events (All)', 'wait_sampling_tot', 'top_wait_sampling_events_htbl', 'wt_smp_all', '<p>Top wait events detected in all backends</p>')
;

-- Query section of regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'sqlela_t', 'sqlsthdr', 100, 'Top SQL by elapsed time', 'Top SQL by elapsed time', 'planning_times', 'top_elapsed_htbl', 'top_ela', NULL),
(1, 'sqlplan_t', 'sqlsthdr', 200, 'Top SQL by planning time', 'Top SQL by planning time', 'planning_times', 'top_plan_time_htbl', 'top_plan', NULL),
(1, 'sqlexec_t', 'sqlsthdr', 300, 'Top SQL by execution time', 'Top SQL by execution time', NULL, 'top_exec_time_htbl', 'top_exec', NULL),
(1, 'sqlcalls', 'sqlsthdr', 400, 'Top SQL by executions', 'Top SQL by executions', NULL, 'top_exec_htbl', 'top_calls', NULL),
(1, 'sqlio_t', 'sqlsthdr', 500, 'Top SQL by I/O wait time', 'Top SQL by I/O wait time', 'io_times', 'top_iowait_htbl', 'top_iowait', NULL),
(1, 'sqlfetch', 'sqlsthdr', 600, 'Top SQL by shared blocks fetched', 'Top SQL by shared blocks fetched', NULL, 'top_shared_blks_fetched_htbl', 'top_pgs_fetched', NULL),
(1, 'sqlshrd', 'sqlsthdr', 700, 'Top SQL by shared blocks read', 'Top SQL by shared blocks read', NULL, 'top_shared_reads_htbl', 'top_shared_reads', NULL),
(1, 'sqlshdir', 'sqlsthdr', 800, 'Top SQL by shared blocks dirtied', 'Top SQL by shared blocks dirtied', NULL, 'top_shared_dirtied_htbl', 'top_shared_dirtied', NULL),
(1, 'sqlshwr', 'sqlsthdr', 900, 'Top SQL by shared blocks written', 'Top SQL by shared blocks written', NULL, 'top_shared_written_htbl', 'top_shared_written', NULL),
(1, 'sqlwalsz', 'sqlsthdr', 1000, 'Top SQL by WAL size', 'Top SQL by WAL size', 'statement_wal_bytes', 'top_wal_size_htbl', 'top_wal_bytes', NULL),
(1, 'sqltmp', 'sqlsthdr', 1100, 'Top SQL by temp usage', 'Top SQL by temp usage', NULL, 'top_temp_htbl', 'top_temp', NULL),
(1, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', 'top_jit_htbl', 'top_jit', NULL),
(1, 'sqlkcachehdr', 'sqlsthdr', 1200, 'rusage statistics', 'rusage statistics', 'kcachestatements', NULL, 'kcache_stat', NULL),
(1, 'sqllist', 'sqlsthdr', 1300, 'Complete list of SQL texts', 'Complete list of SQL texts', NULL, 'report_queries', 'sql_list', NULL)
;

-- rusage section of regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'sqlrusgcpu_t', 'sqlkcachehdr', 100, 'Top SQL by system and user time', 'Top SQL by system and user time', NULL, 'top_cpu_time_htbl', 'kcache_time', NULL),
(1, 'sqlrusgio', 'sqlkcachehdr', 200, 'Top SQL by reads/writes done by filesystem layer', 'Top SQL by reads/writes done by filesystem layer', NULL, 'top_io_filesystem_htbl', 'kcache_reads_writes', NULL)
;

-- Schema objects section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'tblscan', 'objects', 100, 'Top tables by estimated sequentially scanned volume', 'Top tables by estimated sequentially scanned volume', NULL, 'top_scan_tables_htbl', 'scanned_tbl', NULL),
(1, 'tblfetch', 'objects', 200, 'Top tables by blocks fetched', 'Top tables by blocks fetched', NULL, 'tbl_top_fetch_htbl', 'fetch_tbl', NULL),
(1, 'tblrd', 'objects', 300, 'Top tables by blocks read', 'Top tables by blocks read', NULL, 'tbl_top_io_htbl', 'read_tbl', NULL),
(1, 'tbldml', 'objects', 400, 'Top DML tables', 'Top DML tables', NULL, 'top_dml_tables_htbl', 'dml_tbl', NULL),
(1, 'tblud', 'objects', 500, 'Top tables by updated/deleted tuples', 'Top tables by updated/deleted tuples', NULL, 'top_upd_vac_tables_htbl', 'vac_tbl', NULL),
(1, 'tblgrw', 'objects', 600, 'Top growing tables', 'Top growing tables', NULL, 'top_growth_tables_htbl', 'growth_tbl',
  '<ul><li>Sizes in square brackets is based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}'),
(1, 'ixfetch', 'objects', 700, 'Top indexes by blocks fetched', 'Top indexes by blocks fetched', NULL, 'ix_top_fetch_htbl', 'fetch_idx', NULL),
(1, 'ixrd', 'objects', 800, 'Top indexes by blocks read', 'Top indexes by blocks read', NULL, 'ix_top_io_htbl', 'read_idx', NULL),
(1, 'ixgrw', 'objects', 900, 'Top growing indexes', 'Top growing indexes', NULL, 'top_growth_indexes_htbl', 'growth_idx',
  '<ul><li>Sizes in square brackets is based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}'),
(1, 'ixunused', 'objects', 1000, 'Unused indexes', 'Unused indexes', NULL, 'ix_unused_htbl', 'ix_unused',
  '<p>This table contains non-scanned indexes (during report interval), ordered by number of DML '
  'operations on underlying tables. Constraint indexes are excluded.</p>{func_output}')
;

-- Functions section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'func_t', 'funchdr', 100, 'Top functions by total time', 'Top functions by total time', NULL, 'func_top_time_htbl', 'funcs_time_stat', NULL),
(1, 'func_c', 'funchdr', 200, 'Top functions by executions', 'Top functions by executions', NULL, 'func_top_calls_htbl', 'funcs_calls_stat', NULL),
(1, 'func_trg', 'funchdr', 300, 'Top trigger functions by total time', 'Top trigger functions by total time', 'trigger_function_stats', 'func_top_trg_htbl', 'trg_funcs_time_stat', NULL)
;

-- Vacuum section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'vacops', 'vachdr', 100, 'Top tables by vacuum operations', 'Top tables by vacuum operations', NULL, 'top_vacuumed_tables_htbl', 'top_vacuum_cnt_tbl', NULL),
(1, 'anops', 'vachdr', 200, 'Top tables by analyze operations', 'Top tables by analyze operations', NULL, 'top_analyzed_tables_htbl', 'top_analyze_cnt_tbl', NULL),
(1, 'ixvacest', 'vachdr', 300, 'Top indexes by estimated vacuum load', 'Top indexes by estimated vacuum load', NULL, 'top_vacuumed_indexes_htbl', 'top_ix_vacuum_bytes_cnt_tbl', NULL),
(1, 'tblbydead', 'vachdr', 400, 'Top tables by dead tuples ratio', 'Top tables by dead tuples ratio', NULL, 'tbl_top_dead_htbl', 'dead_tbl',
  '<p>Data in this section is not differential. This data is valid for last report sample only.</p>{func_output}'),
(1, 'tblbymod', 'vachdr', 500, 'Top tables by modified tuples ratio', 'Top tables by modified tuples ratio', NULL, 'tbl_top_mods_htbl', 'mod_tbl',
  '<p>Data in this section is not differential. This data is valid for last report sample only.</p>{func_output}')
;

-- Differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'stmt_cmt1', NULL, 100, NULL, NULL, NULL, 'check_stmt_cnt_first_htbl', NULL, '<p><strong>Warning!</strong></p>'
  '<p>First interval contains sample(s) with captured statements count more than 90% of '
  '<i>pg_stat_statements.max</i> parameter.</p>'
  '{func_output}'
  '<p> Consider increasing <i>pg_stat_statements.max</i> parameter.</p>'),
(2, 'stmt_cmt2', NULL, 200, NULL, NULL, NULL, 'check_stmt_cnt_second_htbl', NULL, '<p><strong>Warning!</strong></p>'
  '<p>Second interval contains sample(s) with captured statements count more than 90% of '
  '<i>pg_stat_statements.max</i> parameter.</p>'
  '{func_output}'
  '<p> Consider increasing <i>pg_stat_statements.max</i> parameter.</p>'),
(2, 'srvstat', NULL, 300, 'Server statistics', 'Server statistics', NULL, NULL, 'cl_stat', NULL),
(2, 'sqlsthdr', NULL, 400, 'SQL query statistics', 'SQL query statistics', 'statstatements', NULL, 'sql_stat', NULL),
(2, 'objects', NULL, 500, 'Schema object statistics', 'Schema object statistics', NULL, NULL, 'schema_stat', NULL),
(2, 'funchdr', NULL, 600, 'User function statistics', 'User function statistics', 'function_stats', NULL, 'func_stat', NULL),
(2, 'vachdr', NULL, 700, 'Vacuum-related statistics', 'Vacuum-related statistics', NULL, NULL, 'vacuum_stats', NULL),
(2, 'setings', NULL, 800, 'Cluster settings during the report interval', 'Cluster settings during the report interval', NULL, 'settings_and_changes_diff_htbl', 'pg_settings', NULL),
(2, 'stmt_warn', NULL, 900, NULL, 'Warning!', NULL, 'check_stmt_cnt_all_htbl', NULL, NULL)
;

-- Server section of differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'dbstat1', 'srvstat', 100, 'Database statistics', 'Database statistics', NULL, NULL, 'db_stat', NULL),
(2, 'dbstat2', 'srvstat', 200, NULL, NULL, NULL, 'dbstats_reset_diff_htbl', NULL,
  '<p><b>Warning!</b> Database statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Statistics for listed databases and contained objects might be affected</p>'),
(2, 'dbstat3', 'srvstat', 300, NULL, NULL, NULL, 'dbstats_diff_htbl', NULL, NULL),
(2, 'sesstat', 'srvstat', 400, 'Session statistics by database', 'Session statistics by database', 'sess_stats', 'dbstats_sessions_diff_htbl', 'db_stat_sessions', NULL),
(2, 'stmtstat', 'srvstat', 500, 'Statement statistics by database', 'Statement statistics by database', 'statstatements', 'statements_stats_diff_htbl', 'st_stat', NULL),
(2, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', 'dbagg_jit_stats_diff_htbl', 'dbagg_jit_stat', NULL),
(2, 'div1', 'srvstat', 600, NULL, NULL, NULL, NULL, NULL, '<div>'),
(2, 'clusthdr', 'srvstat', 700, 'Cluster statistics', 'Cluster statistics', NULL, NULL, 'clu_stat',
  '<div style="display:inline-block; margin-right:2em;">{header}'),
(2, 'clustrst', 'srvstat', 800, NULL, NULL, NULL, 'cluster_stats_reset_diff_htbl', NULL,
  '<p><b>Warning!</b> Cluster statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Cluster statistics might be affected</p>'),
(2, 'clust', 'srvstat', 900, NULL, NULL, NULL, 'cluster_stats_diff_htbl', NULL, '{func_output}</div>'),
(2, 'walsthdr', 'srvstat', 1000, 'WAL statistics', 'WAL statistics', 'wal_stats', NULL, 'wal_stat',
  '<div style="display:inline-block; margin-right:2em;">{header}'),
(2, 'walstrst', 'srvstat', 1100, NULL, NULL, 'wal_stats', 'wal_stats_reset_diff_htbl', NULL,
  '<p><b>Warning!</b> WAL statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>WAL statistics might be affected</p>'),
(2, 'walst', 'srvstat', 1200, NULL, NULL, 'wal_stats', 'wal_stats_diff_htbl', NULL, '{func_output}</div>'),
(2, 'div2', 'srvstat', 1300, NULL, NULL, NULL, NULL, NULL, '</div>'),
(2, 'tbspst', 'srvstat', 1400, 'Tablespace statistics', 'Tablespace statistics', NULL, 'tablespaces_stats_diff_htbl', 'tablespace_stat', NULL),
(2, 'wait_sampling_srvstats', 'srvstat', 1500, 'Wait sampling', 'Wait sampling', 'wait_sampling_tot', NULL, 'wait_sampling', NULL),
(2, 'wait_sampling_total', 'wait_sampling_srvstats', 100, 'Wait events types', 'Wait events types', 'wait_sampling_tot', 'wait_sampling_totals_diff_htbl', 'wait_sampling_total', NULL),
(2, 'wait_sampling_statements', 'wait_sampling_srvstats', 200, 'Top wait events (statements)', 'Top wait events (statements)', 'wait_sampling_tot', 'top_wait_sampling_events_diff_htbl', 'wt_smp_stmt', '<p>Top wait events detected in statements execution</p>'),
(2, 'wait_sampling_all', 'wait_sampling_srvstats', 300, 'Top wait events (All)', 'Top wait events (All)', 'wait_sampling_tot', 'top_wait_sampling_events_diff_htbl', 'wt_smp_all', '<p>Top wait events detected in all backends</p>')
;

-- Query section of differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'sqlela_t', 'sqlsthdr', 100, 'Top SQL by elapsed time', 'Top SQL by elapsed time', 'planning_times', 'top_elapsed_diff_htbl', 'top_ela', NULL),
(2, 'sqlplan_t', 'sqlsthdr', 200, 'Top SQL by planning time', 'Top SQL by planning time', 'planning_times', 'top_plan_time_diff_htbl', 'top_plan', NULL),
(2, 'sqlexec_t', 'sqlsthdr', 300, 'Top SQL by execution time', 'Top SQL by execution time', NULL, 'top_exec_time_diff_htbl', 'top_exec', NULL),
(2, 'sqlcalls', 'sqlsthdr', 400, 'Top SQL by executions', 'Top SQL by executions', NULL, 'top_exec_diff_htbl', 'top_calls', NULL),
(2, 'sqlio_t', 'sqlsthdr', 500, 'Top SQL by I/O wait time', 'Top SQL by I/O wait time', 'io_times', 'top_iowait_diff_htbl', 'top_iowait', NULL),
(2, 'sqlfetch', 'sqlsthdr', 600, 'Top SQL by shared blocks fetched', 'Top SQL by shared blocks fetched', NULL, 'top_shared_blks_fetched_diff_htbl', 'top_pgs_fetched', NULL),
(2, 'sqlshrd', 'sqlsthdr', 700, 'Top SQL by shared blocks read', 'Top SQL by shared blocks read', NULL, 'top_shared_reads_diff_htbl', 'top_shared_reads', NULL),
(2, 'sqlshdir', 'sqlsthdr', 800, 'Top SQL by shared blocks dirtied', 'Top SQL by shared blocks dirtied', NULL, 'top_shared_dirtied_diff_htbl', 'top_shared_dirtied', NULL),
(2, 'sqlshwr', 'sqlsthdr', 900, 'Top SQL by shared blocks written', 'Top SQL by shared blocks written', NULL, 'top_shared_written_diff_htbl', 'top_shared_written', NULL),
(2, 'sqlwalsz', 'sqlsthdr', 1000, 'Top SQL by WAL size', 'Top SQL by WAL size', 'statement_wal_bytes', 'top_wal_size_diff_htbl', 'top_wal_bytes', NULL),
(2, 'sqltmp', 'sqlsthdr', 1100, 'Top SQL by temp usage', 'Top SQL by temp usage', NULL, 'top_temp_diff_htbl', 'top_temp', NULL),
(2, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', 'top_jit_diff_htbl', 'top_jit', NULL),
(2, 'sqlkcachehdr', 'sqlsthdr', 1200, 'rusage statistics', 'rusage statistics', 'kcachestatements', NULL, 'kcache_stat', NULL),
(2, 'sqllist', 'sqlsthdr', 1300, 'Complete list of SQL texts', 'Complete list of SQL texts', NULL, 'report_queries', 'sql_list', NULL)
;

-- rusage section of differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'sqlrusgcpu_t', 'sqlkcachehdr', 100, 'Top SQL by system and user time', 'Top SQL by system and user time', NULL, 'top_cpu_time_diff_htbl', 'kcache_time', NULL),
(2, 'sqlrusgio', 'sqlkcachehdr', 200, 'Top SQL by reads/writes done by filesystem layer', 'Top SQL by reads/writes done by filesystem layer', NULL, 'top_io_filesystem_diff_htbl', 'kcache_reads_writes', NULL)
;

-- Schema objects section of a differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'tblscan', 'objects', 100, 'Top tables by estimated sequentially scanned volume', 'Top tables by estimated sequentially scanned volume', NULL, 'top_scan_tables_diff_htbl', 'scanned_tbl', NULL),
(2, 'tblfetch', 'objects', 200, 'Top tables by blocks fetched', 'Top tables by blocks fetched', NULL, 'tbl_top_fetch_diff_htbl', 'fetch_tbl', NULL),
(2, 'tblrd', 'objects', 300, 'Top tables by blocks read', 'Top tables by blocks read', NULL, 'tbl_top_io_diff_htbl', 'read_tbl', NULL),
(2, 'tbldml', 'objects', 400, 'Top DML tables', 'Top DML tables', NULL, 'top_dml_tables_diff_htbl', 'dml_tbl', NULL),
(2, 'tblud', 'objects', 500, 'Top tables by updated/deleted tuples', 'Top tables by updated/deleted tuples', NULL, 'top_upd_vac_tables_diff_htbl', 'vac_tbl', NULL),
(2, 'tblgrw', 'objects', 600, 'Top growing tables', 'Top growing tables', NULL, 'top_growth_tables_diff_htbl', 'growth_tbl',
  '<ul><li>Sizes in square brackets is based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}'),
(2, 'ixfetch', 'objects', 700, 'Top indexes by blocks fetched', 'Top indexes by blocks fetched', NULL, 'ix_top_fetch_diff_htbl', 'fetch_idx', NULL),
(2, 'ixrd', 'objects', 800, 'Top indexes by blocks read', 'Top indexes by blocks read', NULL, 'ix_top_io_diff_htbl', 'read_idx', NULL),
(2, 'ixgrw', 'objects', 900, 'Top growing indexes', 'Top growing indexes', NULL, 'top_growth_indexes_diff_htbl', 'growth_idx',
  '<ul><li>Sizes in square brackets is based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}')
;

-- Functions section of a differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'func_t', 'funchdr', 100, 'Top functions by total time', 'Top functions by total time', NULL, 'func_top_time_diff_htbl', 'funcs_time_stat', NULL),
(2, 'func_c', 'funchdr', 200, 'Top functions by executions', 'Top functions by executions', NULL, 'func_top_calls_diff_htbl', 'funcs_calls_stat', NULL),
(2, 'func_trg', 'funchdr', 300, 'Top trigger functions by total time', 'Top trigger functions by total time', 'trigger_function_stats', 'func_top_trg_diff_htbl', 'trg_funcs_time_stat', NULL)
;

-- Vacuum section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'vacops', 'vachdr', 100, 'Top tables by vacuum operations', 'Top tables by vacuum operations', NULL, 'top_vacuumed_tables_diff_htbl', 'top_vacuum_cnt_tbl', NULL),
(2, 'anops', 'vachdr', 200, 'Top tables by analyze operations', 'Top tables by analyze operations', NULL, 'top_analyzed_tables_diff_htbl', 'top_analyze_cnt_tbl', NULL),
(2, 'ixvacest', 'vachdr', 300, 'Top indexes by estimated vacuum load', 'Top indexes by estimated vacuum load', NULL, 'top_vacuumed_indexes_diff_htbl', 'top_ix_vacuum_bytes_cnt_tbl', NULL)
;
/* ========= Internal functions ========= */

CREATE FUNCTION get_connstr(IN sserver_id integer, INOUT properties jsonb)
SET search_path=@extschema@ SET lock_timeout=300000 AS $$
DECLARE
    server_connstr    text = NULL;
    server_host       text = NULL;
BEGIN
    ASSERT properties IS NOT NULL, 'properties must be not null';
    --Getting server_connstr
    SELECT connstr INTO server_connstr FROM servers n WHERE n.server_id = sserver_id;
    ASSERT server_connstr IS NOT NULL, 'server_id not found';
    /*
    When host= parameter is not specified, connection to unix socket is assumed.
    Unix socket can be in non-default location, so we need to specify it
    */
    IF (SELECT count(*) = 0 FROM regexp_matches(server_connstr,$o$((\s|^)host\s*=)$o$)) AND
      (SELECT count(*) != 0 FROM pg_catalog.pg_settings
      WHERE name = 'unix_socket_directories' AND boot_val != reset_val)
    THEN
      -- Get suitable socket name from available list
      server_host := (SELECT COALESCE(t[1],t[4])
        FROM pg_catalog.pg_settings,
          regexp_matches(reset_val,'("(("")|[^"])+")|([^,]+)','g') AS t
        WHERE name = 'unix_socket_directories' AND boot_val != reset_val
          -- libpq can't handle sockets with comma in their names
          AND position(',' IN COALESCE(t[1],t[4])) = 0
        LIMIT 1
      );
      -- quoted string processing
      IF left(server_host, 1) = '"' AND
        right(server_host, 1) = '"' AND
        (length(server_host) > 1)
      THEN
        server_host := replace(substring(server_host,2,length(server_host)-2),'""','"');
      END IF;
      -- append host parameter to the connection string
      IF server_host IS NOT NULL AND server_host != '' THEN
        server_connstr := concat_ws(server_connstr, format('host=%L',server_host), ' ');
      ELSE
        server_connstr := concat_ws(server_connstr, format('host=%L','localhost'), ' ');
      END IF;
    END IF;

    properties := jsonb_set(properties, '{properties, server_connstr}',
      to_jsonb(server_connstr));
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION jsonb_replace(IN settings jsonb, IN templates jsonb) RETURNS jsonb AS $$
DECLARE
    res_jsonb           jsonb;
    jsontemplkey        text;
    jsondictkey         text;

    c_replace_tab CURSOR(json_scope text) FOR
    SELECT
      '{'||template.key||'}' AS placeholder,
      CASE
        WHEN features.value::boolean THEN template.value
        ELSE COALESCE(template_not.value,'')
      END AS substitution
    FROM jsonb_each_text(settings #> ARRAY[json_scope]) AS features
      JOIN jsonb_each_text(templates) AS template ON (strpos(template.key,features.key||'?') = 1)
      LEFT JOIN jsonb_each_text(templates) AS template_not ON (template_not.key = '!'||template.key)
    ;

    r_replace RECORD;
    jscope    text;
BEGIN
    res_jsonb := templates;
    /* Conditional template placeholders processing
    * based on available report features
    */
    FOREACH jscope IN ARRAY ARRAY['report_properties', 'report_features'] LOOP
      FOR r_replace IN c_replace_tab(jscope) LOOP
        FOR jsontemplkey IN SELECT jsonb_object_keys(res_jsonb) LOOP
          res_jsonb := jsonb_set(res_jsonb, ARRAY[jsontemplkey],
            to_jsonb(replace(res_jsonb #>> ARRAY[jsontemplkey],
            r_replace.placeholder, r_replace.substitution)));
        END LOOP; -- over table templates
      END LOOP; -- over feature-based replacements
    END LOOP; -- over json settings scope
    -- Replacing common html/css placeholders
    FOR jsontemplkey IN SELECT jsonb_object_keys(res_jsonb) LOOP
      FOR jsondictkey IN SELECT jsonb_object_keys(settings #> ARRAY['htbl']) LOOP
        res_jsonb := jsonb_set(res_jsonb, ARRAY[jsontemplkey],
          to_jsonb(replace(res_jsonb #>> ARRAY[jsontemplkey], '{'||jsondictkey||'}',
            settings #> ARRAY['htbl'] #>> ARRAY[jsondictkey])));
      END LOOP;
    END LOOP;

    RETURN res_jsonb;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_sampleids_by_timerange(IN sserver_id integer, IN time_range tstzrange)
RETURNS TABLE (
    start_id    integer,
    end_id      integer
) SET search_path=@extschema@ AS $$
BEGIN
  SELECT min(s1.sample_id),max(s2.sample_id) INTO start_id,end_id FROM
    samples s1 JOIN
    /* Here redundant join condition s1.sample_id < s2.sample_id is needed
     * Otherwise optimizer is using tstzrange(s1.sample_time,s2.sample_time) && time_range
     * as first join condition and some times failes with error
     * ERROR:  range lower bound must be less than or equal to range upper bound
     */
    samples s2 ON (s1.sample_id < s2.sample_id AND s1.server_id = s2.server_id AND s1.sample_id + 1 = s2.sample_id)
  WHERE s1.server_id = sserver_id AND tstzrange(s1.sample_time,s2.sample_time) && time_range;

    IF start_id IS NULL OR end_id IS NULL THEN
      RAISE 'Suitable samples not found';
    END IF;

    RETURN NEXT;
    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_server_by_name(IN server name)
RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    sserver_id     integer;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name=server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found.';
    END IF;

    RETURN sserver_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_baseline_samples(IN sserver_id integer, baseline varchar(25))
RETURNS TABLE (
    start_id    integer,
    end_id      integer
) SET search_path=@extschema@ AS $$
BEGIN
    SELECT min(sample_id), max(sample_id) INTO start_id,end_id
    FROM baselines JOIN bl_samples USING (bl_id,server_id)
    WHERE server_id = sserver_id AND bl_name = baseline;
    IF start_id IS NULL OR end_id IS NULL THEN
      RAISE 'Baseline not found';
    END IF;
    RETURN NEXT;
    RETURN;
END;
$$ LANGUAGE plpgsql;
/* ========= Baseline management functions ========= */

CREATE FUNCTION create_baseline(IN server name, IN baseline varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    baseline_id integer;
    sserver_id     integer;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name=server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found';
    END IF;

    INSERT INTO baselines(server_id,bl_name,keep_until)
    VALUES (sserver_id,baseline,now() + (days || ' days')::interval)
    RETURNING bl_id INTO baseline_id;

    INSERT INTO bl_samples (server_id,sample_id,bl_id)
    SELECT server_id,sample_id,baseline_id
    FROM samples s JOIN servers n USING (server_id)
    WHERE server_id=sserver_id AND sample_id BETWEEN start_id AND end_id;

    RETURN baseline_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_baseline(IN server name, IN baseline varchar(25), IN start_id integer, IN end_id integer, IN days integer) IS 'New baseline by ID''s';

CREATE FUNCTION create_baseline(IN baseline varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    RETURN create_baseline('local',baseline,start_id,end_id,days);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_baseline(IN baseline varchar(25), IN start_id integer, IN end_id integer, IN days integer) IS 'Local server new baseline by ID''s';

CREATE FUNCTION create_baseline(IN server name, IN baseline varchar(25), IN time_range tstzrange, IN days integer = NULL) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
  range_ids record;
BEGIN
  SELECT * INTO STRICT range_ids
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range);

  RETURN create_baseline(server,baseline,range_ids.start_id,range_ids.end_id,days);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_baseline(IN server name, IN baseline varchar(25), IN time_range tstzrange, IN days integer) IS 'New baseline by time range';

CREATE FUNCTION create_baseline(IN baseline varchar(25), IN time_range tstzrange, IN days integer = NULL) RETURNS integer
  SET search_path=@extschema@ AS $$
BEGIN
  RETURN create_baseline('local',baseline,time_range,days);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_baseline(IN baseline varchar(25), IN time_range tstzrange, IN days integer) IS 'Local server new baseline by time range';

CREATE FUNCTION drop_baseline(IN server name, IN baseline varchar(25)) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    del_rows integer;
BEGIN
    DELETE FROM baselines WHERE bl_name = baseline AND server_id IN (SELECT server_id FROM servers WHERE server_name = server);
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION drop_baseline(IN server name, IN baseline varchar(25)) IS 'Drop baseline on server';

CREATE FUNCTION drop_baseline(IN baseline varchar(25)) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    RETURN drop_baseline('local',baseline);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION drop_baseline(IN baseline varchar(25)) IS 'Drop baseline on local server';

CREATE FUNCTION keep_baseline(IN server name, IN baseline varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE baselines SET keep_until = now() + (days || ' days')::interval WHERE (baseline IS NULL OR bl_name = baseline) AND server_id IN (SELECT server_id FROM servers WHERE server_name = server);
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION  keep_baseline(IN server name, IN baseline varchar(25), IN days integer) IS 'Set new baseline retention on server';

CREATE FUNCTION keep_baseline(IN baseline varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    RETURN keep_baseline('local',baseline,days);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION keep_baseline(IN baseline varchar(25), IN days integer) IS 'Set new baseline retention on local server';

CREATE FUNCTION show_baselines(IN server name = 'local')
RETURNS TABLE (
       baseline varchar(25),
       min_sample integer,
       max_sample integer,
       keep_until_time timestamp (0) with time zone
) SET search_path=@extschema@ AS $$
    SELECT bl_name as baseline,min_sample_id,max_sample_id, keep_until
    FROM baselines b JOIN
        (SELECT server_id,bl_id,min(sample_id) min_sample_id,max(sample_id) max_sample_id FROM bl_samples GROUP BY server_id,bl_id) b_agg
    USING (server_id,bl_id)
    WHERE server_id IN (SELECT server_id FROM servers WHERE server_name = server)
    ORDER BY min_sample_id;
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_baselines(IN server name) IS 'Show server baselines (local server assumed if omitted)';
/* ========= Server functions ========= */

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

CREATE FUNCTION rename_server(IN server name, IN server_new_name name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET server_name = server_new_name WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION rename_server(IN server name, IN server_new_name name) IS 'Rename existing server';

CREATE FUNCTION set_server_connstr(IN server name, IN server_connstr text) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET connstr = server_connstr WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_server_connstr(IN server name, IN server_connstr text) IS 'Update server connection string';

CREATE FUNCTION set_server_description(IN server name, IN description text) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET server_description = description WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_server_description(IN server name, IN description text) IS 'Update server description';

CREATE FUNCTION set_server_max_sample_age(IN server name, IN max_sample_age integer) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET max_sample_age = set_server_max_sample_age.max_sample_age WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_server_max_sample_age(IN server name, IN max_sample_age integer) IS 'Update server max_sample_age period';

CREATE FUNCTION enable_server(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET enabled = TRUE WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION enable_server(IN server name) IS 'Enable existing server (will be included in take_sample() call)';

CREATE FUNCTION disable_server(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET enabled = FALSE WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION disable_server(IN server name) IS 'Disable existing server (will be excluded from take_sample() call)';

CREATE FUNCTION set_server_db_exclude(IN server name, IN exclude_db name[]) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET db_exclude = exclude_db WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_server_db_exclude(IN server name, IN exclude_db name[]) IS 'Exclude databases from object stats collection. Useful in RDS.';

CREATE FUNCTION set_server_size_sampling(IN server name, IN window_start time with time zone = NULL,
  IN window_duration interval hour to second = NULL, IN sample_interval interval day to minute = NULL)
RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers
    SET
      (size_smp_wnd_start, size_smp_wnd_dur, size_smp_interval) =
      (window_start, window_duration, sample_interval)
    WHERE
      server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION set_server_size_sampling(IN server name, IN window_start time with time zone,
  IN window_duration interval hour to second, IN sample_interval interval day to minute)
IS 'Set relation sizes sampling settings for a server';

CREATE FUNCTION show_servers()
RETURNS TABLE(server_name name, connstr text, enabled boolean, description text)
SET search_path=@extschema@ AS $$
    SELECT server_name, connstr, enabled, server_description FROM servers;
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_servers() IS 'Displays all servers';

CREATE FUNCTION show_servers_size_sampling()
RETURNS TABLE (
  server_name name,
  window_start time with time zone,
  window_end time with time zone,
  window_duration interval hour to second,
  sample_interval interval day to minute
)
SET search_path=@extschema@ AS $$
  SELECT
    server_name,
    size_smp_wnd_start,
    size_smp_wnd_start + size_smp_wnd_dur,
    size_smp_wnd_dur,
    size_smp_interval
  FROM
    servers
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_servers_size_sampling() IS
  'Displays relation sizes sampling settings for all servers';

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

CREATE FUNCTION delete_samples(IN server_name name, IN start_id integer = NULL, IN end_id integer = NULL)
RETURNS integer
SET search_path=@extschema@ AS $$
  SELECT delete_samples(server_id, start_id, end_id)
  FROM servers s
  WHERE s.server_name = delete_samples.server_name
$$ LANGUAGE sql;
COMMENT ON FUNCTION delete_samples(name, integer, integer) IS
  'Manually deletes server samples for provided server name. By default deletes all samples';

CREATE FUNCTION delete_samples(IN start_id integer = NULL, IN end_id integer = NULL)
RETURNS integer
SET search_path=@extschema@ AS $$
  SELECT delete_samples(server_id, start_id, end_id)
  FROM servers s
  WHERE s.server_name = 'local'
$$ LANGUAGE sql;
COMMENT ON FUNCTION delete_samples(integer, integer) IS
  'Manually deletes server samples of local server. By default deletes all samples';

CREATE FUNCTION delete_samples(IN server_name name, IN time_range tstzrange)
RETURNS integer
SET search_path=@extschema@ AS $$
  SELECT delete_samples(server_id, min(sample_id), max(sample_id))
  FROM servers srv JOIN samples smp USING (server_id)
  WHERE
    srv.server_name = delete_samples.server_name AND
    delete_samples.time_range @> smp.sample_time
  GROUP BY server_id
$$ LANGUAGE sql;
COMMENT ON FUNCTION delete_samples(name, integer, integer) IS
  'Manually deletes server samples for provided server name and time interval';

CREATE FUNCTION delete_samples(IN time_range tstzrange)
RETURNS integer
SET search_path=@extschema@ AS $$
  SELECT delete_samples('local', time_range);
$$ LANGUAGE sql;
COMMENT ON FUNCTION delete_samples(name, integer, integer) IS
  'Manually deletes server samples for time interval on local server';
SELECT create_server('local','dbname='||current_database()||' port='||current_setting('port'));
/* ==== Export and import functions ==== */

DROP FUNCTION IF EXISTS export_data(name, integer, integer, boolean);
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

DROP FUNCTION IF EXISTS import_data;
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

CREATE FUNCTION mark_pg_stat_statements(IN sserver_id integer, IN s_id integer, IN topn integer)
RETURNS void
SET search_path=@extschema@ AS $$
  -- Mark statements to include in a sample
  UPDATE last_stat_statements ust
  SET in_sample = true
  FROM
    (SELECT
      cur.server_id,
      cur.sample_id,
      cur.userid,
      cur.datid,
      cur.queryid,
      cur.toplevel,
      cur.wal_bytes IS NOT NULL AS wal_avail,
      cur.total_plan_time IS NOT NULL AS plantime_avail,
      COALESCE(cur.blk_read_time,0) + COALESCE(cur.blk_write_time,0) > 0 AS iotime_avail,
      row_number() over (ORDER BY cur.total_plan_time + cur.total_exec_time DESC NULLS LAST) AS time_rank,
      row_number() over (ORDER BY cur.total_plan_time DESC NULLS LAST) AS plan_time_rank,
      row_number() over (ORDER BY cur.total_exec_time DESC NULLS LAST) AS exec_time_rank,
      row_number() over (ORDER BY cur.calls DESC NULLS LAST) AS calls_rank,
      row_number() over (ORDER BY cur.blk_read_time + cur.blk_write_time DESC NULLS LAST) AS io_time_rank,
      row_number() over (ORDER BY cur.shared_blks_hit + cur.shared_blks_read DESC NULLS LAST) AS gets_rank,
      row_number() over (ORDER BY cur.shared_blks_read DESC NULLS LAST) AS read_rank,
      row_number() over (ORDER BY cur.shared_blks_dirtied DESC NULLS LAST) AS dirtied_rank,
      row_number() over (ORDER BY cur.shared_blks_written DESC NULLS LAST) AS written_rank,
      row_number() over (ORDER BY cur.temp_blks_written + cur.local_blks_written DESC NULLS LAST) AS tempw_rank,
      row_number() over (ORDER BY cur.temp_blks_read + cur.local_blks_read DESC NULLS LAST) AS tempr_rank,
      row_number() over (ORDER BY cur.wal_bytes DESC NULLS LAST) AS wal_rank
    FROM
      last_stat_statements cur
      -- In case of statements in already dropped database
      JOIN sample_stat_database db USING (server_id, sample_id, datid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    ) diff
  WHERE
    (
      (wal_avail AND wal_rank <= topn) OR
      (plantime_avail AND least(time_rank, plan_time_rank) <= topn) OR
      (iotime_avail AND io_time_rank <= topn) OR
      least(
        exec_time_rank,
        calls_rank,
        gets_rank,
        read_rank,
        dirtied_rank,
        written_rank,
        tempw_rank,
        tempr_rank
      ) <= topn
    )
    AND
    (ust.server_id ,ust.sample_id, ust.userid, ust.datid, ust.queryid, ust.toplevel, ust.in_sample) =
    (diff.server_id, diff.sample_id, diff.userid, diff.datid, diff.queryid, diff.toplevel, false);

  -- Mark rusage stats to include in a sample
  UPDATE last_stat_statements ust
  SET in_sample = true
  FROM
    (SELECT
      cur.server_id,
      cur.sample_id,
      cur.userid,
      cur.datid,
      cur.queryid,
      cur.toplevel,
      COALESCE(plan_user_time, 0.0) + COALESCE(plan_system_time, 0.0) > 0.0 AS plans_stats_avail,
      row_number() OVER (ORDER BY plan_user_time + plan_system_time DESC NULLS LAST) AS plan_cpu_time_rank,
      row_number() OVER (ORDER BY exec_user_time + exec_system_time DESC NULLS LAST) AS exec_cpu_time_rank,
      row_number() OVER (ORDER BY plan_reads + plan_writes DESC NULLS LAST) AS plan_io_rank,
      row_number() OVER (ORDER BY exec_reads + exec_writes DESC NULLS LAST) AS exec_io_rank
    FROM
      last_stat_kcache cur
      -- In case of statements in already dropped database
      JOIN sample_stat_database db USING (server_id, sample_id, datid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    ) diff
  WHERE
    (
      (plans_stats_avail AND least(plan_cpu_time_rank, plan_io_rank) <= topn) OR
      least(
        exec_cpu_time_rank,
        exec_io_rank
      ) <= topn
    )
    AND
    (ust.server_id, ust.sample_id, ust.userid, ust.datid, ust.queryid, ust.toplevel, ust.in_sample) =
    (diff.server_id, diff.sample_id, diff.userid, diff.datid, diff.queryid, diff.toplevel, false);
$$ LANGUAGE sql;

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
/* pg_wait_sampling support */

CREATE FUNCTION collect_pg_wait_sampling_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
BEGIN
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_wait_sampling'
      )
      WHEN '1.1' THEN
        PERFORM collect_pg_wait_sampling_stats_11(properties, sserver_id, s_id, topn);
      ELSE
        NULL;
    END CASE;

END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION collect_pg_wait_sampling_stats_11(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  qres      record;

  st_query  text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    st_query := format('SELECT w.*,row_number() OVER () as weid '
      'FROM ( '
        'SELECT '
          'event_type,'
          'event,'
          'sum(count * current_setting(''pg_wait_sampling.profile_period'')::bigint) as tot_waited, '
          'sum(count * current_setting(''pg_wait_sampling.profile_period'')::bigint) '
            'FILTER (WHERE queryid IS NOT NULL AND queryid != 0) as stmt_waited '
        'FROM '
          '%1$I.pg_wait_sampling_profile '
        'GROUP BY '
          'event_type, '
          'event) as w',
      (
        SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extnamespace text)
        WHERE extname = 'pg_stat_statements'
      )
    );

    INSERT INTO wait_sampling_total(
      server_id,
      sample_id,
      sample_wevnt_id,
      event_type,
      event,
      tot_waited,
      stmt_waited
    )
    SELECT
      sserver_id,
      s_id,
      dbl.weid,
      dbl.event_type,
      dbl.event,
      dbl.tot_waited,
      dbl.stmt_waited
    FROM
      dblink('server_connection', st_query) AS dbl(
        event_type    text,
        event         text,
        tot_waited    bigint,
        stmt_waited   bigint,
        weid          integer
      );

    -- reset wait sampling profile
    SELECT * INTO qres FROM dblink('server_connection',
      format('SELECT %1$I.pg_wait_sampling_reset_profile()',
        (
          SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extnamespace text)
          WHERE extname = 'pg_wait_sampling'
        )
      )
    ) AS t(res char(1));

END;
$$ LANGUAGE plpgsql;
/* ========= Sample functions ========= */

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

CREATE FUNCTION take_sample(IN server name, IN skip_sizes boolean = NULL) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    sserver_id    integer;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name = server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found';
    ELSE
        RETURN take_sample(sserver_id, skip_sizes);
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample(IN server name, IN skip_sizes boolean) IS
  'Statistics sample creation function (by server name)';

CREATE FUNCTION take_sample_subset(IN sets_cnt integer = 1, IN current_set integer = 0) RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
DECLARE
    c_servers CURSOR FOR
      SELECT server_id,server_name FROM (
        SELECT server_id,server_name, row_number() OVER () AS srv_rn
        FROM servers WHERE enabled
        ) AS t1
      WHERE srv_rn % sets_cnt = current_set;
    server_sampleres        integer;
    etext               text := '';
    edetail             text := '';
    econtext            text := '';

    qres          RECORD;
    start_clock   timestamp (2) with time zone;
BEGIN
    IF sets_cnt IS NULL OR sets_cnt < 1 THEN
      RAISE 'sets_cnt value is invalid. Must be positive';
    END IF;
    IF current_set IS NULL OR current_set < 0 OR current_set > sets_cnt - 1 THEN
      RAISE 'current_cnt value is invalid. Must be between 0 and sets_cnt - 1';
    END IF;
    FOR qres IN c_servers LOOP
        BEGIN
            start_clock := clock_timestamp()::timestamp (2) with time zone;
            server := qres.server_name;
            server_sampleres := take_sample(qres.server_id, NULL);
            elapsed := clock_timestamp()::timestamp (2) with time zone - start_clock;
            CASE server_sampleres
              WHEN 0 THEN
                result := 'OK';
              ELSE
                result := 'FAIL';
            END CASE;
            RETURN NEXT;
        EXCEPTION
            WHEN OTHERS THEN
                BEGIN
                    GET STACKED DIAGNOSTICS etext = MESSAGE_TEXT,
                        edetail = PG_EXCEPTION_DETAIL,
                        econtext = PG_EXCEPTION_CONTEXT;
                    result := format (E'%s\n%s\n%s', etext, econtext, edetail);
                    elapsed := clock_timestamp()::timestamp (2) with time zone - start_clock;
                    RETURN NEXT;
                END;
        END;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample_subset(IN sets_cnt integer, IN current_set integer) IS
  'Statistics sample creation function (for subset of enabled servers). Used for simplification of parallel sample collection.';

CREATE FUNCTION take_sample() RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
  SELECT * FROM take_sample_subset(1,0);
$$ LANGUAGE sql;

COMMENT ON FUNCTION take_sample() IS 'Statistics sample creation function (for all enabled servers). Must be explicitly called periodically.';

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

CREATE FUNCTION sample_dbobj_delta(IN properties jsonb, IN sserver_id integer, IN s_id integer,
  IN topn integer, IN skip_sizes boolean) RETURNS jsonb AS $$
DECLARE
    qres    record;
    result  jsonb := sample_dbobj_delta.properties;
BEGIN

    /* This function will calculate statistics increments for database objects
    * and store top objects values in sample.
    * Due to relations between objects we need to mark top objects (and their
    * dependencies) first, and calculate increments later
    */
    IF (properties #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(properties,'{timings,calculate tables stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Marking functions
    UPDATE last_stat_user_functions ulf
    SET in_sample = true
    FROM
        (SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.funcid,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.total_time - COALESCE(lst.total_time,0) DESC) time_rank,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.self_time - COALESCE(lst.self_time,0) DESC) stime_rank,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.calls - COALESCE(lst.calls,0) DESC) calls_rank
        FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
          LEFT OUTER JOIN last_stat_database dblst ON
            (dblst.server_id, dblst.datid, dblst.sample_id, dblst.stats_reset) =
            (dbcur.server_id, dbcur.datid, dbcur.sample_id - 1, dbcur.stats_reset)
          LEFT OUTER JOIN last_stat_user_functions lst ON
            (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
            (dblst.server_id, dblst.sample_id, dblst.datid, cur.funcid)
        WHERE
            (cur.server_id, cur.sample_id) =
            (sserver_id, s_id)
            AND cur.calls - COALESCE(lst.calls,0) > 0) diff
    WHERE
      least(
        time_rank,
        calls_rank,
        stime_rank
      ) <= topn
      AND (ulf.server_id, ulf.sample_id, ulf.datid, ulf.funcid) =
        (diff.server_id, diff.sample_id, diff.datid, diff.funcid);

    -- Marking indexes
    UPDATE last_stat_indexes uli
    SET in_sample = true
    FROM
      (SELECT
          cur.server_id,
          cur.sample_id,
          cur.datid,
          cur.indexrelid,
          -- Index ranks
          row_number() OVER (ORDER BY cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) DESC) read_rank,
          row_number() OVER (ORDER BY cur.idx_blks_read+cur.idx_blks_hit-
            COALESCE(lst.idx_blks_read+lst.idx_blks_hit,0) DESC) gets_rank,
          row_number() OVER (PARTITION BY cur.idx_scan - COALESCE(lst.idx_scan,0) = 0
            ORDER BY tblcur.n_tup_ins - COALESCE(tbllst.n_tup_ins,0) +
            tblcur.n_tup_upd - COALESCE(tbllst.n_tup_upd,0) +
            tblcur.n_tup_del - COALESCE(tbllst.n_tup_del,0) DESC) dml_unused_rank,
          row_number() OVER (ORDER BY (tblcur.vacuum_count - COALESCE(tbllst.vacuum_count,0) +
            tblcur.autovacuum_count - COALESCE(tbllst.autovacuum_count,0)) *
              -- Coalesce is used here in case of skipped size collection
              COALESCE(cur.relsize,lst.relsize) DESC) vacuum_bytes_rank
      FROM last_stat_indexes cur JOIN last_stat_tables tblcur USING (server_id, sample_id, datid, relid)
        JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
        LEFT OUTER JOIN last_stat_database dblst ON
          (dblst.server_id, dblst.datid, dblst.sample_id, dblst.stats_reset) =
          (dbcur.server_id, dbcur.datid, dbcur.sample_id - 1, dbcur.stats_reset)
        LEFT OUTER JOIN last_stat_indexes lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
          (dblst.server_id, dblst.sample_id, dblst.datid, cur.relid, cur.indexrelid)
        LEFT OUTER JOIN last_stat_tables tbllst ON
          (tbllst.server_id, tbllst.sample_id, tbllst.datid, tbllst.relid) =
          (dblst.server_id, dblst.sample_id, dblst.datid, lst.relid)
      WHERE
        (cur.server_id, cur.sample_id) =
        (sserver_id, s_id)
      ) diff
    WHERE
      (least(
        read_rank,
        gets_rank,
        vacuum_bytes_rank
      ) <= topn
      OR (dml_unused_rank <= topn AND idx_scan = 0))
      AND (uli.server_id, uli.sample_id, uli.datid, uli.indexrelid, uli.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, diff.indexrelid, false);

    -- Growth rank is to be calculated independently of database stats_reset value
    UPDATE last_stat_indexes uli
    SET in_sample = true
    FROM
      (SELECT
          cur.server_id,
          cur.sample_id,
          cur.datid,
          cur.indexrelid,
          cur.relsize IS NOT NULL AS relsize_avail,
          cur.relpages_bytes IS NOT NULL AS relpages_avail,
          -- Index ranks
          row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize,0) DESC NULLS LAST) growth_rank,
          row_number() OVER (ORDER BY cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) DESC NULLS LAST) pagegrowth_rank
      FROM last_stat_indexes cur
        LEFT OUTER JOIN last_stat_indexes lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
          (cur.server_id, cur.sample_id - 1, cur.datid, cur.relid, cur.indexrelid)
      WHERE
        (cur.server_id, cur.sample_id) =
        (sserver_id, s_id)
      ) diff
    WHERE
      ((relsize_avail AND growth_rank <= topn) OR
      ((NOT relsize_avail) AND relpages_avail AND pagegrowth_rank <= topn))
      AND (uli.server_id, uli.sample_id, uli.datid, uli.indexrelid, uli.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, diff.indexrelid, false);

    -- Marking tables
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM (
      SELECT
          cur.server_id AS server_id,
          cur.sample_id AS sample_id,
          cur.datid AS datid,
          cur.relid AS relid,
          tcur.relid AS toastrelid,
          -- Seq. scanned blocks rank
          row_number() OVER (ORDER BY
            (cur.seq_scan - COALESCE(lst.seq_scan,0)) * cur.relsize +
            (tcur.seq_scan - COALESCE(tlst.seq_scan,0)) * tcur.relsize DESC) scan_rank,
          row_number() OVER (ORDER BY cur.n_tup_ins + cur.n_tup_upd + cur.n_tup_del -
            COALESCE(lst.n_tup_ins + lst.n_tup_upd + lst.n_tup_del, 0) +
            COALESCE(tcur.n_tup_ins + tcur.n_tup_upd + tcur.n_tup_del, 0) -
            COALESCE(tlst.n_tup_ins + tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) dml_rank,
          row_number() OVER (ORDER BY cur.n_tup_upd+cur.n_tup_del -
            COALESCE(lst.n_tup_upd + lst.n_tup_del, 0) +
            COALESCE(tcur.n_tup_upd + tcur.n_tup_del, 0) -
            COALESCE(tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) vacuum_dml_rank,
          row_number() OVER (ORDER BY
            cur.n_dead_tup / NULLIF(cur.n_live_tup+cur.n_dead_tup, 0)
            DESC NULLS LAST) dead_pct_rank,
          row_number() OVER (ORDER BY
            cur.n_mod_since_analyze / NULLIF(cur.n_live_tup, 0)
            DESC NULLS LAST) mod_pct_rank,
          -- Read rank
          row_number() OVER (ORDER BY
            cur.heap_blks_read - COALESCE(lst.heap_blks_read,0) +
            cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) +
            cur.toast_blks_read - COALESCE(lst.toast_blks_read,0) +
            cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0) DESC) read_rank,
          -- Page processing rank
          row_number() OVER (ORDER BY cur.heap_blks_read+cur.heap_blks_hit+cur.idx_blks_read+cur.idx_blks_hit+
            cur.toast_blks_read+cur.toast_blks_hit+cur.tidx_blks_read+cur.tidx_blks_hit-
            COALESCE(lst.heap_blks_read+lst.heap_blks_hit+lst.idx_blks_read+lst.idx_blks_hit+
            lst.toast_blks_read+lst.toast_blks_hit+lst.tidx_blks_read+lst.tidx_blks_hit, 0) DESC) gets_rank,
          -- Vacuum rank
          row_number() OVER (ORDER BY cur.vacuum_count - COALESCE(lst.vacuum_count, 0) +
            cur.autovacuum_count - COALESCE(lst.autovacuum_count, 0) DESC) vacuum_rank,
          row_number() OVER (ORDER BY cur.analyze_count - COALESCE(lst.analyze_count,0) +
            cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) DESC) analyze_rank
      FROM
        -- main relations diff
        last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
        LEFT OUTER JOIN last_stat_database dblst ON
          (dblst.server_id, dblst.datid, dblst.sample_id, dblst.stats_reset) =
          (dbcur.server_id, dbcur.datid, dbcur.sample_id - 1, dbcur.stats_reset)
        LEFT OUTER JOIN last_stat_tables lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
          (dblst.server_id, dblst.sample_id, dblst.datid, cur.relid)
        -- toast relations diff
        LEFT OUTER JOIN last_stat_tables tcur ON
          (tcur.server_id, tcur.sample_id, tcur.datid, tcur.relid) =
          (dbcur.server_id, dbcur.sample_id, dbcur.datid, cur.reltoastrelid)
        LEFT OUTER JOIN last_stat_tables tlst ON
          (tlst.server_id, tlst.sample_id, tlst.datid, tlst.relid) =
          (dblst.server_id, dblst.sample_id, dblst.datid, lst.reltoastrelid)
      WHERE
        (cur.server_id, cur.sample_id, cur.in_sample) =
        (sserver_id, s_id, false)
        AND cur.relkind IN ('r','m')) diff
    WHERE
      least(
        scan_rank,
        dml_rank,
        dead_pct_rank,
        mod_pct_rank,
        vacuum_dml_rank,
        read_rank,
        gets_rank,
        vacuum_rank,
        analyze_rank
      ) <= topn
      AND (ulst.server_id, ulst.sample_id, ulst.datid, ulst.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, false)
      AND (ulst.relid = diff.relid OR ulst.relid = diff.toastrelid);

    -- Growth rank is to be calculated independently of database stats_reset value
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM (
      SELECT
          cur.server_id AS server_id,
          cur.sample_id AS sample_id,
          cur.datid AS datid,
          cur.relid AS relid,
          tcur.relid AS toastrelid,
          cur.relsize IS NOT NULL AS relsize_avail,
          cur.relpages_bytes IS NOT NULL AS relpages_avail,
          row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize, 0) +
            COALESCE(tcur.relsize,0) - COALESCE(tlst.relsize, 0) DESC NULLS LAST) growth_rank,
          row_number() OVER (ORDER BY cur.relpages_bytes - COALESCE(lst.relpages_bytes, 0) +
            COALESCE(tcur.relpages_bytes,0) - COALESCE(tlst.relpages_bytes, 0) DESC NULLS LAST) pagegrowth_rank
      FROM
        -- main relations diff
        last_stat_tables cur
        LEFT OUTER JOIN last_stat_tables lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
          (cur.server_id, cur.sample_id - 1, cur.datid, cur.relid)
        -- toast relations diff
        LEFT OUTER JOIN last_stat_tables tcur ON
          (tcur.server_id, tcur.sample_id, tcur.datid, tcur.relid) =
          (cur.server_id, cur.sample_id, cur.datid, cur.reltoastrelid)
        LEFT OUTER JOIN last_stat_tables tlst ON
          (tlst.server_id, tlst.sample_id, tlst.datid, tlst.relid) =
          (lst.server_id, lst.sample_id, lst.datid, lst.reltoastrelid)
      WHERE cur.sample_id=s_id AND cur.server_id=sserver_id
        AND cur.relkind IN ('r','m')) diff
    WHERE
      ((relsize_avail AND growth_rank <= topn) OR
      ((NOT relsize_avail) AND relpages_avail AND pagegrowth_rank <= topn))
      AND (ulst.server_id, ulst.sample_id, ulst.datid, in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, false)
      AND (ulst.relid = diff.relid OR ulst.relid = diff.toastrelid);

    /* Also mark tables having marked indexes on them including main
    * table in case of a TOAST index and TOAST table if index is on
    * main table
    */
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM
      last_stat_indexes ix
      JOIN last_stat_tables tbl USING (server_id, sample_id, datid, relid)
      LEFT JOIN last_stat_tables mtbl ON
        (mtbl.server_id, mtbl.sample_id, mtbl.datid, mtbl.reltoastrelid) =
        (tbl.server_id, tbl.sample_id, tbl.datid, tbl.relid)
    WHERE
      (ix.server_id, ix.sample_id, ix.in_sample) =
      (sserver_id, s_id, true)
      AND (ulst.server_id, ulst.sample_id, ulst.datid, ulst.in_sample) =
        (tbl.server_id, tbl.sample_id, tbl.datid, false)
      AND ulst.relid IN (tbl.relid, tbl.reltoastrelid, mtbl.relid);

    -- Insert marked objects statistics increments
    -- New table names
    INSERT INTO tables_list AS itl (
      server_id,
      last_sample_id,
      datid,
      relid,
      relkind,
      reltoastrelid,
      schemaname,
      relname
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.relid,
      cur.relkind,
      NULLIF(cur.reltoastrelid, 0),
      cur.schemaname,
      cur.relname
    FROM
      last_stat_tables cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_tables_list DO
      UPDATE SET
        (last_sample_id, reltoastrelid, schemaname, relname) =
        (EXCLUDED.last_sample_id, EXCLUDED.reltoastrelid, EXCLUDED.schemaname, EXCLUDED.relname)
      WHERE
        (itl.last_sample_id, itl.reltoastrelid, itl.schemaname, itl.relname) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.reltoastrelid, EXCLUDED.schemaname, EXCLUDED.relname);

    -- Tables
    INSERT INTO sample_stat_tables (
      server_id,
      sample_id,
      datid,
      relid,
      tablespaceid,
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
      relpages_bytes,
      relpages_bytes_diff
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.relid AS relid,
      cur.tablespaceid AS tablespaceid,
      cur.seq_scan - COALESCE(lst.seq_scan,0) AS seq_scan,
      cur.seq_tup_read - COALESCE(lst.seq_tup_read,0) AS seq_tup_read,
      cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
      cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
      cur.n_tup_ins - COALESCE(lst.n_tup_ins,0) AS n_tup_ins,
      cur.n_tup_upd - COALESCE(lst.n_tup_upd,0) AS n_tup_upd,
      cur.n_tup_del - COALESCE(lst.n_tup_del,0) AS n_tup_del,
      cur.n_tup_hot_upd - COALESCE(lst.n_tup_hot_upd,0) AS n_tup_hot_upd,
      cur.n_live_tup AS n_live_tup,
      cur.n_dead_tup AS n_dead_tup,
      cur.n_mod_since_analyze AS n_mod_since_analyze,
      cur.n_ins_since_vacuum AS n_ins_since_vacuum,
      cur.last_vacuum AS last_vacuum,
      cur.last_autovacuum AS last_autovacuum,
      cur.last_analyze AS last_analyze,
      cur.last_autoanalyze AS last_autoanalyze,
      cur.vacuum_count - COALESCE(lst.vacuum_count,0) AS vacuum_count,
      cur.autovacuum_count - COALESCE(lst.autovacuum_count,0) AS autovacuum_count,
      cur.analyze_count - COALESCE(lst.analyze_count,0) AS analyze_count,
      cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) AS autoanalyze_count,
      cur.heap_blks_read - COALESCE(lst.heap_blks_read,0) AS heap_blks_read,
      cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0) AS heap_blks_hit,
      cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
      cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
      cur.toast_blks_read - COALESCE(lst.toast_blks_read,0) AS toast_blks_read,
      cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0) AS toast_blks_hit,
      cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0) AS tidx_blks_read,
      cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0) AS tidx_blks_hit,
      cur.relsize AS relsize,
      cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
      cur.relpages_bytes AS relpages_bytes,
      cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) AS relpages_bytes_diff
    FROM
      last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid, dblst.stats_reset) =
        (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid, dbcur.stats_reset)
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (dblst.server_id, dblst.sample_id, dblst.datid, cur.relid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Update incorrectly calculated relation growth in case of database stats reset
    UPDATE sample_stat_tables usst
    SET
      relsize_diff = cur.relsize - COALESCE(lst.relsize,0),
      relpages_bytes_diff = cur.relpages_bytes - COALESCE(lst.relpages_bytes,0)
    FROM
      last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid)
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (dblst.server_id, dblst.sample_id, dblst.datid, cur.relid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
      AND dblst.stats_reset != dbcur.stats_reset
      AND (usst.server_id, usst.sample_id, usst.datid, usst.relid) =
        (cur.server_id, cur.sample_id, cur.datid, cur.relid);

    -- Total table stats
    INSERT INTO sample_stat_tables_total(
      server_id,
      sample_id,
      datid,
      tablespaceid,
      relkind,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_tup_ins,
      n_tup_upd,
      n_tup_del,
      n_tup_hot_upd,
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
      relsize_diff
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      cur.tablespaceid,
      cur.relkind,
      sum(cur.seq_scan - COALESCE(lst.seq_scan,0)),
      sum(cur.seq_tup_read - COALESCE(lst.seq_tup_read,0)),
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.n_tup_ins - COALESCE(lst.n_tup_ins,0)),
      sum(cur.n_tup_upd - COALESCE(lst.n_tup_upd,0)),
      sum(cur.n_tup_del - COALESCE(lst.n_tup_del,0)),
      sum(cur.n_tup_hot_upd - COALESCE(lst.n_tup_hot_upd,0)),
      sum(cur.vacuum_count - COALESCE(lst.vacuum_count,0)),
      sum(cur.autovacuum_count - COALESCE(lst.autovacuum_count,0)),
      sum(cur.analyze_count - COALESCE(lst.analyze_count,0)),
      sum(cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0)),
      sum(cur.heap_blks_read - COALESCE(lst.heap_blks_read,0)),
      sum(cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      sum(cur.toast_blks_read - COALESCE(lst.toast_blks_read,0)),
      sum(cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0)),
      sum(cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0)),
      sum(cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0)),
      CASE
        WHEN skip_sizes THEN NULL
        ELSE sum(cur.relsize - COALESCE(lst.relsize,0))
      END
    FROM last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.datid, dblst.sample_id, dblst.stats_reset) =
        (dbcur.server_id, dbcur.datid, dbcur.sample_id - 1, dbcur.stats_reset)
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (dblst.server_id, dblst.sample_id, dblst.datid, cur.relid)
    WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid;

    IF NOT skip_sizes THEN
    /* Update incorrectly calculated aggregated tables growth in case of
     * database statistics reset
     */
      UPDATE sample_stat_tables_total usstt
      SET relsize_diff = calc.relsize_diff
      FROM (
          SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.relkind,
            cur.tablespaceid,
            sum(cur.relsize - COALESCE(lst.relsize,0)) AS relsize_diff
          FROM last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
            JOIN last_stat_database dblst ON
              (dblst.server_id, dblst.sample_id, dblst.datid) =
              (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid)
            LEFT OUTER JOIN last_stat_tables lst ON
              (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
              (dblst.server_id, dblst.sample_id, dblst.datid, cur.relid)
          WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
            AND dblst.stats_reset != dbcur.stats_reset
          GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid
        ) calc
      WHERE (usstt.server_id, usstt.sample_id, usstt.datid, usstt.relkind, usstt.tablespaceid) =
        (calc.server_id, calc.sample_id, calc.datid, calc.relkind, calc.tablespaceid);

    END IF;
    /*
    Preserve previous relation sizes in if we couldn't collect
    size this time (for example, due to locked relation)*/
    UPDATE last_stat_tables cur
    SET relsize = lst.relsize
    FROM last_stat_tables lst
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
      AND (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
      (cur.server_id, cur.sample_id - 1, cur.datid, cur.relid)
      AND cur.relsize IS NULL;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate tables stats,end}',to_jsonb(clock_timestamp()));
      result := jsonb_set(result,'{timings,calculate indexes stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- New index names
    INSERT INTO indexes_list AS iil (
      server_id,
      last_sample_id,
      datid,
      indexrelid,
      relid,
      schemaname,
      indexrelname
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.indexrelid,
      cur.relid,
      cur.schemaname,
      cur.indexrelname
    FROM
      last_stat_indexes cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_indexes_list DO
      UPDATE SET
        (last_sample_id, relid, schemaname, indexrelname) =
        (EXCLUDED.last_sample_id, EXCLUDED.relid, EXCLUDED.schemaname, EXCLUDED.indexrelname)
      WHERE
        (iil.last_sample_id, iil.relid, iil.schemaname, iil.indexrelname) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.relid, EXCLUDED.schemaname, EXCLUDED.indexrelname);

    -- Index stats
    INSERT INTO sample_stat_indexes (
      server_id,
      sample_id,
      datid,
      indexrelid,
      tablespaceid,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch,
      idx_blks_read,
      idx_blks_hit,
      relsize,
      relsize_diff,
      indisunique,
      relpages_bytes,
      relpages_bytes_diff
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.indexrelid AS indexrelid,
      cur.tablespaceid AS tablespaceid,
      cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
      cur.idx_tup_read - COALESCE(lst.idx_tup_read,0) AS idx_tup_read,
      cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
      cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
      cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
      cur.relsize,
      cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
      cur.indisunique,
      cur.relpages_bytes AS relpages_bytes,
      cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) AS relpages_bytes_diff
    FROM
      last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid, dblst.stats_reset) =
        (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid, dbcur.stats_reset)
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
        (dblst.server_id, dblst.sample_id, dblst.datid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Update incorrectly calculated relation growth in case of database stats reset
    UPDATE sample_stat_indexes ussi
    SET
      relsize_diff = cur.relsize - COALESCE(lst.relsize,0),
      relpages_bytes_diff = cur.relpages_bytes - COALESCE(lst.relpages_bytes,0)
    FROM
      last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid)
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
        (dblst.server_id, dblst.sample_id, dblst.datid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
      AND dblst.stats_reset != dbcur.stats_reset
      AND (ussi.server_id, ussi.sample_id, ussi.datid, ussi.indexrelid) =
        (cur.server_id, cur.sample_id, cur.datid, cur.indexrelid);

    -- Total indexes stats
    INSERT INTO sample_stat_indexes_total(
      server_id,
      sample_id,
      datid,
      tablespaceid,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch,
      idx_blks_read,
      idx_blks_hit,
      relsize_diff
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      cur.tablespaceid,
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_read - COALESCE(lst.idx_tup_read,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      CASE
        WHEN skip_sizes THEN NULL
        ELSE sum(cur.relsize - COALESCE(lst.relsize,0))
      END
    FROM last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid, dblst.stats_reset) =
        (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid, dbcur.stats_reset)
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
        (dblst.server_id, dblst.sample_id, dblst.datid, cur.relid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.tablespaceid;

    /* Update incorrectly calculated aggregated index growth in case of
     * database statistics reset
     */
    IF NOT skip_sizes THEN
      UPDATE sample_stat_indexes_total ussit
      SET relsize_diff = calc.relsize_diff
      FROM (
          SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.tablespaceid,
            sum(cur.relsize - COALESCE(lst.relsize,0)) AS relsize_diff
          FROM last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
            JOIN last_stat_database dblst ON
              (dblst.server_id, dblst.sample_id, dblst.datid) =
              (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid)
            LEFT OUTER JOIN last_stat_indexes lst ON
              (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
              (dblst.server_id, dblst.sample_id, dblst.datid, cur.indexrelid)
          WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
            AND dblst.stats_reset != dbcur.stats_reset
          GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.tablespaceid
        ) calc
      WHERE (ussit.server_id, ussit.sample_id, ussit.datid, ussit.tablespaceid) =
        (calc.server_id, calc.sample_id, calc.datid, calc.tablespaceid);
    END IF;
    /*
    Preserve previous relation sizes in if we couldn't collect
    size this time (for example, due to locked relation)*/
    UPDATE last_stat_indexes cur
    SET relsize = lst.relsize
    FROM last_stat_indexes lst
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
      AND (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
      (cur.server_id, cur.sample_id - 1, cur.datid, cur.indexrelid)
      AND cur.relsize IS NULL;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate indexes stats,end}',to_jsonb(clock_timestamp()));
      result := jsonb_set(result,'{timings,calculate functions stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- New function names
    INSERT INTO funcs_list AS ifl (
      server_id,
      last_sample_id,
      datid,
      funcid,
      schemaname,
      funcname,
      funcargs
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.funcid,
      cur.schemaname,
      cur.funcname,
      cur.funcargs
    FROM
      last_stat_user_functions cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_funcs_list DO
      UPDATE SET
        (last_sample_id, funcid, schemaname, funcname, funcargs) =
        (EXCLUDED.last_sample_id, EXCLUDED.funcid, EXCLUDED.schemaname,
          EXCLUDED.funcname, EXCLUDED.funcargs)
      WHERE
        (ifl.last_sample_id, ifl.funcid, ifl.schemaname,
          ifl.funcname, ifl.funcargs) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.funcid, EXCLUDED.schemaname,
          EXCLUDED.funcname, EXCLUDED.funcargs);

    -- Function stats
    INSERT INTO sample_stat_user_functions (
      server_id,
      sample_id,
      datid,
      funcid,
      calls,
      total_time,
      self_time,
      trg_fn
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.funcid,
      cur.calls - COALESCE(lst.calls,0) AS calls,
      cur.total_time - COALESCE(lst.total_time,0) AS total_time,
      cur.self_time - COALESCE(lst.self_time,0) AS self_time,
      cur.trg_fn
    FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid, dblst.stats_reset) =
        (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid, dbcur.stats_reset)
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
        (dblst.server_id, dblst.sample_id, dblst.datid, cur.funcid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Total functions stats
    INSERT INTO sample_stat_user_func_total(
      server_id,
      sample_id,
      datid,
      calls,
      total_time,
      trg_fn
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      sum(cur.calls - COALESCE(lst.calls,0)),
      sum(cur.total_time - COALESCE(lst.total_time,0)),
      cur.trg_fn
    FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid, dblst.stats_reset) =
        (dbcur.server_id, dbcur.sample_id - 1, dbcur.datid, dbcur.stats_reset)
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
        (dblst.server_id, dblst.sample_id, dblst.datid, cur.funcid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.trg_fn;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate functions stats,end}',to_jsonb(clock_timestamp()));
    END IF;

    -- Clear data in last_ tables, holding data only for next diff sample
    DELETE FROM last_stat_tables WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_indexes WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_user_functions WHERE server_id=sserver_id AND sample_id != s_id;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION show_samples(IN server name,IN days integer = NULL)
RETURNS TABLE(
    sample integer,
    sample_time timestamp (0) with time zone,
    sizes_collected boolean,
    dbstats_reset timestamp (0) with time zone,
    bgwrstats_reset timestamp (0) with time zone,
    archstats_reset timestamp (0) with time zone)
SET search_path=@extschema@ AS $$
  SELECT
    s.sample_id,
    s.sample_time,
    count(relsize_diff) > 0 AS sizes_collected,
    max(nullif(db1.stats_reset,coalesce(db2.stats_reset,db1.stats_reset))) AS dbstats_reset,
    max(nullif(bgwr1.stats_reset,coalesce(bgwr2.stats_reset,bgwr1.stats_reset))) AS bgwrstats_reset,
    max(nullif(arch1.stats_reset,coalesce(arch2.stats_reset,arch1.stats_reset))) AS archstats_reset
  FROM samples s JOIN servers n USING (server_id)
    JOIN sample_stat_database db1 USING (server_id,sample_id)
    JOIN sample_stat_cluster bgwr1 USING (server_id,sample_id)
    JOIN sample_stat_tables_total USING (server_id,sample_id)
    LEFT OUTER JOIN sample_stat_archiver arch1 USING (server_id,sample_id)
    LEFT OUTER JOIN sample_stat_database db2 ON (db1.server_id = db2.server_id AND db1.datid = db2.datid AND db2.sample_id = db1.sample_id - 1)
    LEFT OUTER JOIN sample_stat_cluster bgwr2 ON (bgwr1.server_id = bgwr2.server_id AND bgwr2.sample_id = bgwr1.sample_id - 1)
    LEFT OUTER JOIN sample_stat_archiver arch2 ON (arch1.server_id = arch2.server_id AND arch2.sample_id = arch1.sample_id - 1)
  WHERE (days IS NULL OR s.sample_time > now() - (days || ' days')::interval)
    AND server_name = server
  GROUP BY s.sample_id, s.sample_time
  ORDER BY s.sample_id ASC
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_samples(IN server name,IN days integer) IS 'Display available server samples';

CREATE FUNCTION show_samples(IN days integer = NULL)
RETURNS TABLE(
    sample integer,
    sample_time timestamp (0) with time zone,
    sizes_collected boolean,
    dbstats_reset timestamp (0) with time zone,
    clustats_reset timestamp (0) with time zone,
    archstats_reset timestamp (0) with time zone)
SET search_path=@extschema@ AS $$
    SELECT * FROM show_samples('local',days);
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_samples(IN days integer) IS 'Display available samples for local server';

CREATE FUNCTION get_sized_bounds(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  left_bound    integer,
  right_bound   integer
)
SET search_path=@extschema@ AS $$
SELECT
  left_bound.sample_id AS left_bound,
  right_bound.sample_id AS right_bound
FROM (
    SELECT
      sample_id
    FROM
      sample_stat_tables_total
    WHERE
      server_id = sserver_id
      AND sample_id >= end_id
    GROUP BY
      sample_id
    HAVING
      count(relsize_diff) > 0
    ORDER BY sample_id ASC
    LIMIT 1
  ) right_bound,
  (
    SELECT
      sample_id
    FROM
      sample_stat_tables_total
    WHERE
      server_id = sserver_id
      AND sample_id <= start_id
    GROUP BY
      sample_id
    HAVING
      count(relsize_diff) > 0
    ORDER BY sample_id DESC
    LIMIT 1
  ) left_bound
$$ LANGUAGE sql;
/* ==== Backward compatibility functions ====*/
CREATE FUNCTION snapshot() RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
SELECT * FROM take_sample()
$$ LANGUAGE SQL;

CREATE FUNCTION snapshot(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    RETURN take_sample(server);
END;
$$ LANGUAGE plpgsql;
/* ===== Cluster stats functions ===== */

CREATE FUNCTION cluster_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        server_id               integer,
        checkpoints_timed     bigint,
        checkpoints_req       bigint,
        checkpoint_write_time double precision,
        checkpoint_sync_time  double precision,
        buffers_checkpoint    bigint,
        buffers_clean         bigint,
        buffers_backend       bigint,
        buffers_backend_fsync bigint,
        maxwritten_clean      bigint,
        buffers_alloc         bigint,
        wal_size              bigint,
        archived_count        bigint,
        failed_count          bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id as server_id,
        sum(checkpoints_timed)::bigint as checkpoints_timed,
        sum(checkpoints_req)::bigint as checkpoints_req,
        sum(checkpoint_write_time)::double precision as checkpoint_write_time,
        sum(checkpoint_sync_time)::double precision as checkpoint_sync_time,
        sum(buffers_checkpoint)::bigint as buffers_checkpoint,
        sum(buffers_clean)::bigint as buffers_clean,
        sum(buffers_backend)::bigint as buffers_backend,
        sum(buffers_backend_fsync)::bigint as buffers_backend_fsync,
        sum(maxwritten_clean)::bigint as maxwritten_clean,
        sum(buffers_alloc)::bigint as buffers_alloc,
        sum(wal_size)::bigint as wal_size,
        sum(archived_count)::bigint as archived_count,
        sum(failed_count)::bigint as failed_count
    FROM sample_stat_cluster st
        LEFT OUTER JOIN sample_stat_archiver sa USING (server_id, sample_id)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        sample_id               integer,
        bgwriter_stats_reset  timestamp with time zone,
        archiver_stats_reset  timestamp with time zone
)
SET search_path=@extschema@ AS $$
  SELECT
      bgwr1.sample_id as sample_id,
      nullif(bgwr1.stats_reset,bgwr0.stats_reset),
      nullif(sta1.stats_reset,sta0.stats_reset)
  FROM sample_stat_cluster bgwr1
      LEFT OUTER JOIN sample_stat_archiver sta1 USING (server_id,sample_id)
      JOIN sample_stat_cluster bgwr0 ON (bgwr1.server_id = bgwr0.server_id AND bgwr1.sample_id = bgwr0.sample_id + 1)
      LEFT OUTER JOIN sample_stat_archiver sta0 ON (sta1.server_id = sta0.server_id AND sta1.sample_id = sta0.sample_id + 1)
  WHERE bgwr1.server_id = sserver_id AND bgwr1.sample_id BETWEEN start_id + 1 AND end_id
    AND
      COALESCE(
        nullif(bgwr1.stats_reset,bgwr0.stats_reset),
        nullif(sta1.stats_reset,sta0.stats_reset)
      ) IS NOT NULL
  ORDER BY bgwr1.sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stats_reset_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR (start1_id integer, end1_id integer) FOR
    SELECT
        sample_id,
        bgwriter_stats_reset,
        archiver_stats_reset
    FROM cluster_stats_reset(sserver_id, start1_id, end1_id)
    ORDER BY COALESCE(bgwriter_stats_reset,archiver_stats_reset) ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Sample</th>'
            '<th>BGWriter reset time</th>'
            '<th>Archiver reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl',
        '<tr>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['sample_tpl'],
            r_result.sample_id,
            r_result.bgwriter_stats_reset,
            r_result.archiver_stats_reset
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION cluster_stats_reset_diff_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer) FOR
    SELECT
        interval_num,
        sample_id,
        bgwriter_stats_reset,
        archiver_stats_reset
    FROM
      (SELECT 1 AS interval_num, sample_id, bgwriter_stats_reset, archiver_stats_reset
        FROM cluster_stats_reset(sserver_id,start1_id,end1_id)
      UNION ALL
      SELECT 2 AS interval_num, sample_id, bgwriter_stats_reset, archiver_stats_reset
        FROM cluster_stats_reset(sserver_id,start2_id,end2_id)) AS samples
    ORDER BY interval_num, COALESCE(bgwriter_stats_reset, archiver_stats_reset) ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>I</th>'
            '<th>Sample</th>'
            '<th>BGWriter reset time</th>'
            '<th>Archiver reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl1',
        '<tr {interval1}>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'sample_tpl2',
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
      CASE r_result.interval_num
        WHEN 1 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl1'],
              r_result.sample_id,
              r_result.bgwriter_stats_reset,
              r_result.archiver_stats_reset
          );
        WHEN 2 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl2'],
              r_result.sample_id,
              r_result.bgwriter_stats_reset,
              r_result.archiver_stats_reset
          );
        END CASE;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION cluster_stats_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR (start1_id integer, end1_id integer) FOR
    SELECT
        NULLIF(checkpoints_timed, 0) as checkpoints_timed,
        NULLIF(checkpoints_req, 0) as checkpoints_req,
        NULLIF(checkpoint_write_time, 0.0) as checkpoint_write_time,
        NULLIF(checkpoint_sync_time, 0.0) as checkpoint_sync_time,
        NULLIF(buffers_checkpoint, 0) as buffers_checkpoint,
        NULLIF(buffers_clean, 0) as buffers_clean,
        NULLIF(buffers_backend, 0) as buffers_backend,
        NULLIF(buffers_backend_fsync, 0) as buffers_backend_fsync,
        NULLIF(maxwritten_clean, 0) as maxwritten_clean,
        NULLIF(buffers_alloc, 0) as buffers_alloc,
        NULLIF(wal_size, 0) as wal_size,
        NULLIF(archived_count, 0) as archived_count,
        NULLIF(failed_count, 0) as failed_count
    FROM cluster_stats(sserver_id, start1_id, end1_id);

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Metric</th>'
            '<th>Value</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'val_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer
      )
    LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Scheduled checkpoints',r_result.checkpoints_timed);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Requested checkpoints',r_result.checkpoints_req);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint write time (s)',round(cast(r_result.checkpoint_write_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint sync time (s)',round(cast(r_result.checkpoint_sync_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoints buffers written',r_result.buffers_checkpoint);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Background buffers written',r_result.buffers_clean);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend buffers written',r_result.buffers_backend);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend fsync count',r_result.buffers_backend_fsync);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Number of buffers allocated',r_result.buffers_alloc);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL generated',pg_size_pretty(r_result.wal_size));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL segments archived',r_result.archived_count);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL segments archive failed',r_result.failed_count);
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION cluster_stats_diff_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer) FOR
    SELECT
        NULLIF(stat1.checkpoints_timed, 0) as checkpoints_timed1,
        NULLIF(stat1.checkpoints_req, 0) as checkpoints_req1,
        NULLIF(stat1.checkpoint_write_time, 0.0) as checkpoint_write_time1,
        NULLIF(stat1.checkpoint_sync_time, 0.0) as checkpoint_sync_time1,
        NULLIF(stat1.buffers_checkpoint, 0) as buffers_checkpoint1,
        NULLIF(stat1.buffers_clean, 0) as buffers_clean1,
        NULLIF(stat1.buffers_backend, 0) as buffers_backend1,
        NULLIF(stat1.buffers_backend_fsync, 0) as buffers_backend_fsync1,
        NULLIF(stat1.maxwritten_clean, 0) as maxwritten_clean1,
        NULLIF(stat1.buffers_alloc, 0) as buffers_alloc1,
        NULLIF(stat1.wal_size, 0) as wal_size1,
        NULLIF(stat1.archived_count, 0) as archived_count1,
        NULLIF(stat1.failed_count, 0) as failed_count1,
        NULLIF(stat2.checkpoints_timed, 0) as checkpoints_timed2,
        NULLIF(stat2.checkpoints_req, 0) as checkpoints_req2,
        NULLIF(stat2.checkpoint_write_time, 0.0) as checkpoint_write_time2,
        NULLIF(stat2.checkpoint_sync_time, 0.0) as checkpoint_sync_time2,
        NULLIF(stat2.buffers_checkpoint, 0) as buffers_checkpoint2,
        NULLIF(stat2.buffers_clean, 0) as buffers_clean2,
        NULLIF(stat2.buffers_backend, 0) as buffers_backend2,
        NULLIF(stat2.buffers_backend_fsync, 0) as buffers_backend_fsync2,
        NULLIF(stat2.maxwritten_clean, 0) as maxwritten_clean2,
        NULLIF(stat2.buffers_alloc, 0) as buffers_alloc2,
        NULLIF(stat2.wal_size, 0) as wal_size2,
        NULLIF(stat2.archived_count, 0) as archived_count2,
        NULLIF(stat2.failed_count, 0) as failed_count2
    FROM cluster_stats(sserver_id,start1_id,end1_id) stat1
        FULL OUTER JOIN cluster_stats(sserver_id,start2_id,end2_id) stat2 USING (server_id);

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Metric</th>'
            '<th {title1}>Value (1)</th>'
            '<th {title2}>Value (2)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'val_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {interval1}><div {value}>%s</div></td>'
          '<td {interval2}><div {value}>%s</div></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Scheduled checkpoints',r_result.checkpoints_timed1,r_result.checkpoints_timed2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Requested checkpoints',r_result.checkpoints_req1,r_result.checkpoints_req2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint write time (s)',
            round(cast(r_result.checkpoint_write_time1/1000 as numeric),2),
            round(cast(r_result.checkpoint_write_time2/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint sync time (s)',
            round(cast(r_result.checkpoint_sync_time1/1000 as numeric),2),
            round(cast(r_result.checkpoint_sync_time2/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoints buffers written',r_result.buffers_checkpoint1,r_result.buffers_checkpoint2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Background buffers written',r_result.buffers_clean1,r_result.buffers_clean2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend buffers written',r_result.buffers_backend1,r_result.buffers_backend2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend fsync count',r_result.buffers_backend_fsync1,r_result.buffers_backend_fsync2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean1,r_result.maxwritten_clean2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Number of buffers allocated',r_result.buffers_alloc1,r_result.buffers_alloc2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL generated',
          pg_size_pretty(r_result.wal_size1),pg_size_pretty(r_result.wal_size2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL segments archived',r_result.archived_count1,r_result.archived_count2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL segments archive failed',r_result.failed_count1,r_result.failed_count2);
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
/* ========= Reporting functions ========= */

/* ========= Cluster databases report functions ========= */
CREATE FUNCTION profile_checkavail_io_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have I/O times collected for report interval
  SELECT COALESCE(sum(blk_read_time), 0) + COALESCE(sum(blk_write_time), 0) > 0
  FROM sample_stat_database sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION dbstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id         integer,
    datid             oid,
    dbname            name,
    xact_commit       bigint,
    xact_rollback     bigint,
    blks_read         bigint,
    blks_hit          bigint,
    tup_returned      bigint,
    tup_fetched       bigint,
    tup_inserted      bigint,
    tup_updated       bigint,
    tup_deleted       bigint,
    temp_files        bigint,
    temp_bytes        bigint,
    datsize_delta     bigint,
    deadlocks         bigint,
    checksum_failures bigint,
    checksum_last_failure  timestamp with time zone,
    blk_read_time     double precision,
    blk_write_time    double precision
  )
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.datid AS datid,
        st.datname AS dbname,
        sum(xact_commit)::bigint AS xact_commit,
        sum(xact_rollback)::bigint AS xact_rollback,
        sum(blks_read)::bigint AS blks_read,
        sum(blks_hit)::bigint AS blks_hit,
        sum(tup_returned)::bigint AS tup_returned,
        sum(tup_fetched)::bigint AS tup_fetched,
        sum(tup_inserted)::bigint AS tup_inserted,
        sum(tup_updated)::bigint AS tup_updated,
        sum(tup_deleted)::bigint AS tup_deleted,
        sum(temp_files)::bigint AS temp_files,
        sum(temp_bytes)::bigint AS temp_bytes,
        sum(datsize_delta)::bigint AS datsize_delta,
        sum(deadlocks)::bigint AS deadlocks,
        sum(checksum_failures)::bigint AS checksum_failures,
        max(checksum_last_failure)::timestamp with time zone AS checksum_last_failure,
        sum(blk_read_time)/1000::double precision AS blk_read_time,
        sum(blk_write_time)/1000::double precision AS blk_write_time
    FROM sample_stat_database st
    WHERE st.server_id = sserver_id AND NOT datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.datid, st.datname
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_sessionstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there is table sizes collected in both bounds
  SELECT
    count(session_time) +
    count(active_time) +
    count(idle_in_transaction_time) +
    count(sessions) +
    count(sessions_abandoned) +
    count(sessions_fatal) +
    count(sessions_killed) > 0
  FROM sample_stat_database
  WHERE
    server_id = sserver_id
    AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_sessions(IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS TABLE(
    server_id         integer,
    datid             oid,
    dbname            name,
    session_time      double precision,
    active_time       double precision,
    idle_in_transaction_time  double precision,
    sessions            bigint,
    sessions_abandoned  bigint,
    sessions_fatal    bigint,
    sessions_killed   bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.datid AS datid,
        st.datname AS dbname,
        sum(session_time)::double precision AS xact_commit,
        sum(active_time)::double precision AS xact_rollback,
        sum(idle_in_transaction_time)::double precision AS blks_read,
        sum(sessions)::bigint AS blks_hit,
        sum(sessions_abandoned)::bigint AS tup_returned,
        sum(sessions_fatal)::bigint AS tup_fetched,
        sum(sessions_killed)::bigint AS tup_inserted
    FROM sample_stat_database st
    WHERE st.server_id = sserver_id AND NOT datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.datid, st.datname
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  datname       name,
  stats_reset   timestamp with time zone,
  sample_id       integer
)
SET search_path=@extschema@ AS $$
    SELECT
        st1.datname,
        st1.stats_reset,
        st1.sample_id
    FROM sample_stat_database st1
        LEFT JOIN sample_stat_database st0 ON
          (st0.server_id = st1.server_id AND st0.sample_id = st1.sample_id - 1 AND st0.datid = st1.datid)
    WHERE st1.server_id = sserver_id AND NOT st1.datistemplate AND st1.sample_id BETWEEN start_id + 1 AND end_id
      AND nullif(st1.stats_reset,st0.stats_reset) IS NOT NULL
    ORDER BY sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer) FOR
    SELECT
        datname,
        sample_id,
        stats_reset
    FROM dbstats_reset(sserver_id, start1_id, end1_id)
    ORDER BY stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Database</th>'
            '<th>Sample</th>'
            '<th>Reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl',
      '<tr>'
        '<td>%s</td>'
        '<td {value}>%s</td>'
        '<td {value}>%s</td>'
      '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
      (report_context #>> '{report_properties,start1_id}')::integer,
      (report_context #>> '{report_properties,end1_id}')::integer)
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['sample_tpl'],
            r_result.datname,
            r_result.sample_id,
            r_result.stats_reset
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_reset_diff_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer, start2_id integer, end2_id integer) FOR
    SELECT
        interval_num,
        datname,
        sample_id,
        stats_reset
    FROM
      (SELECT 1 AS interval_num, datname, sample_id, stats_reset
        FROM dbstats_reset(sserver_id, start1_id, end1_id)
      UNION ALL
      SELECT 2 AS interval_num, datname, sample_id, stats_reset
        FROM dbstats_reset(sserver_id, start2_id, end2_id)) AS samples
    ORDER BY interval_num, stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>I</th>'
            '<th>Database</th>'
            '<th>Sample</th>'
            '<th>Reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl1',
        '<tr {interval1}>'
          '<td {label} {title1}>1</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'sample_tpl2',
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
      CASE r_result.interval_num
        WHEN 1 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl1'],
              r_result.datname,
              r_result.sample_id,
              r_result.stats_reset
          );
        WHEN 2 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl2'],
              r_result.datname,
              r_result.sample_id,
              r_result.stats_reset
          );
        END CASE;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer) FOR
    SELECT
        COALESCE(st.dbname,'Total') as dbname,
        NULLIF(sum(st.xact_commit), 0) as xact_commit,
        NULLIF(sum(st.xact_rollback), 0) as xact_rollback,
        NULLIF(sum(st.blks_read), 0) as blks_read,
        NULLIF(sum(st.blks_hit), 0) as blks_hit,
        NULLIF(sum(st.tup_returned), 0) as tup_returned,
        NULLIF(sum(st.tup_fetched), 0) as tup_fetched,
        NULLIF(sum(st.tup_inserted), 0) as tup_inserted,
        NULLIF(sum(st.tup_updated), 0) as tup_updated,
        NULLIF(sum(st.tup_deleted), 0) as tup_deleted,
        NULLIF(sum(st.temp_files), 0) as temp_files,
        pg_size_pretty(NULLIF(sum(st.temp_bytes), 0)) AS temp_bytes,
        pg_size_pretty(NULLIF(sum(st_last.datsize), 0)) AS datsize,
        pg_size_pretty(NULLIF(sum(st.datsize_delta), 0)) AS datsize_delta,
        NULLIF(sum(st.deadlocks), 0) as deadlocks,
        (sum(st.blks_hit)*100/NULLIF(sum(st.blks_hit)+sum(st.blks_read),0))::double precision AS blks_hit_pct,
        NULLIF(sum(st.checksum_failures), 0) as checksum_failures,
        max(st.checksum_last_failure) as checksum_last_failure,
        NULLIF(sum(st.blk_read_time), 0) as blk_read_time,
        NULLIF(sum(st.blk_write_time), 0) as blk_write_time
    FROM dbstats(sserver_id, start1_id, end1_id) st
      LEFT OUTER JOIN sample_stat_database st_last ON
        (st_last.server_id = st.server_id AND st_last.datid = st.datid
          AND st_last.sample_id = end1_id)
    GROUP BY ROLLUP(st.dbname)
    ORDER BY st.dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th colspan="3">Transactions</th>'
            '{checksum_fail_detected?checksum_fail_hdr1}'
            '<th colspan="3">Block statistics</th>'
            '{io_times?io_times_hdr1}'
            '<th colspan="5">Tuples</th>'
            '<th colspan="2">Temp files</th>'
            '<th rowspan="2" title="Database size as is was at the moment of last sample in report interval">Size</th>'
            '<th rowspan="2" title="Database size increment during report interval">Growth</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of transactions in this database that have been committed">Commits</th>'
            '<th title="Number of transactions in this database that have been rolled back">Rollbacks</th>'
            '<th title="Number of deadlocks detected in this database">Deadlocks</th>'
            '{checksum_fail_detected?checksum_fail_hdr2}'
            '<th title="Buffer cache hit ratio">Hit(%)</th>'
            '<th title="Number of disk blocks read in this database">Read</th>'
            '<th title="Number of times disk blocks were found already in the buffer cache">Hit</th>'
            '{io_times?io_times_hdr2}'
            '<th title="Number of rows returned by queries in this database">Ret</th>'
            '<th title="Number of rows fetched by queries in this database">Fet</th>'
            '<th title="Number of rows inserted by queries in this database">Ins</th>'
            '<th title="Number of rows updated by queries in this database">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Total amount of data written to temporary files by queries in this database">Size</th>'
            '<th title="Number of temporary files created by queries in this database">Files</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr>'
          '<td>%1$s</td>'
          '<td {value}>%2$s</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '{checksum_fail_detected?checksum_fail_row}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '{io_times?io_times_row}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
        '</tr>',
      'checksum_fail_detected?checksum_fail_hdr1',
        '<th colspan="2">Checksums</th>',
      'checksum_fail_detected?checksum_fail_hdr2',
        '<th title="Number of block checksum failures detected">Failures</th>'
        '<th title="Last checksum filure detected">Last</th>',
      'checksum_fail_detected?checksum_fail_row',
        '<td {value}><strong>%5$s</strong></td>'
        '<td {value}><strong>%6$s</strong></td>',
      'io_times?io_times_hdr1',
        '<th colspan="2">Block I/O times</th>',
      'io_times?io_times_hdr2',
        '<th title="Time spent reading data file blocks by backends, in seconds">Read</th>'
        '<th title="Time spent writing data file blocks by backends, in seconds">Write</th>',
      'io_times?io_times_row',
        '<td {value}>%19$s</td>'
        '<td {value}>%20$s</td>'
      );
          -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
      (report_context #>> '{report_properties,start1_id}')::integer,
      (report_context #>> '{report_properties,end1_id}')::integer
    )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            r_result.xact_commit,
            r_result.xact_rollback,
            r_result.deadlocks,
            r_result.checksum_failures,
            r_result.checksum_last_failure::text,
            round(CAST(r_result.blks_hit_pct AS numeric),2),
            r_result.blks_read,
            r_result.blks_hit,
            r_result.tup_returned,
            r_result.tup_fetched,
            r_result.tup_inserted,
            r_result.tup_updated,
            r_result.tup_deleted,
            r_result.temp_bytes,
            r_result.temp_files,
            r_result.datsize,
            r_result.datsize_delta,
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer)
    FOR
    SELECT
        COALESCE(COALESCE(dbs1.dbname,dbs2.dbname),'Total') AS dbname,
        NULLIF(sum(dbs1.xact_commit), 0) AS xact_commit1,
        NULLIF(sum(dbs1.xact_rollback), 0) AS xact_rollback1,
        NULLIF(sum(dbs1.blks_read), 0) AS blks_read1,
        NULLIF(sum(dbs1.blks_hit), 0) AS blks_hit1,
        NULLIF(sum(dbs1.tup_returned), 0) AS tup_returned1,
        NULLIF(sum(dbs1.tup_fetched), 0) AS tup_fetched1,
        NULLIF(sum(dbs1.tup_inserted), 0) AS tup_inserted1,
        NULLIF(sum(dbs1.tup_updated), 0) AS tup_updated1,
        NULLIF(sum(dbs1.tup_deleted), 0) AS tup_deleted1,
        NULLIF(sum(dbs1.temp_files), 0) AS temp_files1,
        pg_size_pretty(NULLIF(sum(dbs1.temp_bytes), 0)) AS temp_bytes1,
        pg_size_pretty(NULLIF(sum(st_last1.datsize), 0)) AS datsize1,
        pg_size_pretty(NULLIF(sum(dbs1.datsize_delta), 0)) AS datsize_delta1,
        NULLIF(sum(dbs1.deadlocks), 0) AS deadlocks1,
        (sum(dbs1.blks_hit)*100/NULLIF(sum(dbs1.blks_hit)+sum(dbs1.blks_read),0))::double precision AS blks_hit_pct1,
        NULLIF(sum(dbs1.checksum_failures), 0) as checksum_failures1,
        max(dbs1.checksum_last_failure) as checksum_last_failure1,
        NULLIF(sum(dbs1.blk_read_time), 0) as blk_read_time1,
        NULLIF(sum(dbs1.blk_write_time), 0) as blk_write_time1,
        NULLIF(sum(dbs2.xact_commit), 0) AS xact_commit2,
        NULLIF(sum(dbs2.xact_rollback), 0) AS xact_rollback2,
        NULLIF(sum(dbs2.blks_read), 0) AS blks_read2,
        NULLIF(sum(dbs2.blks_hit), 0) AS blks_hit2,
        NULLIF(sum(dbs2.tup_returned), 0) AS tup_returned2,
        NULLIF(sum(dbs2.tup_fetched), 0) AS tup_fetched2,
        NULLIF(sum(dbs2.tup_inserted), 0) AS tup_inserted2,
        NULLIF(sum(dbs2.tup_updated), 0) AS tup_updated2,
        NULLIF(sum(dbs2.tup_deleted), 0) AS tup_deleted2,
        NULLIF(sum(dbs2.temp_files), 0) AS temp_files2,
        pg_size_pretty(NULLIF(sum(dbs2.temp_bytes), 0)) AS temp_bytes2,
        pg_size_pretty(NULLIF(sum(st_last2.datsize), 0)) AS datsize2,
        pg_size_pretty(NULLIF(sum(dbs2.datsize_delta), 0)) AS datsize_delta2,
        NULLIF(sum(dbs2.deadlocks), 0) AS deadlocks2,
        (sum(dbs2.blks_hit)*100/NULLIF(sum(dbs2.blks_hit)+sum(dbs2.blks_read),0))::double precision AS blks_hit_pct2,
        NULLIF(sum(dbs2.checksum_failures), 0) as checksum_failures2,
        max(dbs2.checksum_last_failure) as checksum_last_failure2,
        NULLIF(sum(dbs2.blk_read_time), 0) as blk_read_time2,
        NULLIF(sum(dbs2.blk_write_time), 0) as blk_write_time2
    FROM dbstats(sserver_id,start1_id,end1_id) dbs1
      FULL OUTER JOIN dbstats(sserver_id,start2_id,end2_id) dbs2
        USING (server_id, datid)
      LEFT OUTER JOIN sample_stat_database st_last1 ON
        (st_last1.server_id = dbs1.server_id AND st_last1.datid = dbs1.datid AND st_last1.sample_id =
        end1_id)
      LEFT OUTER JOIN sample_stat_database st_last2 ON
        (st_last2.server_id = dbs2.server_id AND st_last2.datid = dbs2.datid AND st_last2.sample_id =
        end2_id)
    GROUP BY ROLLUP(COALESCE(dbs1.dbname,dbs2.dbname))
    ORDER BY COALESCE(dbs1.dbname,dbs2.dbname) NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="3">Transactions</th>'
            '{checksum_fail_detected?checksum_fail_hdr1}'
            '<th colspan="3">Block statistics</th>'
            '{io_times?io_times_hdr1}'
            '<th colspan="5">Tuples</th>'
            '<th colspan="2">Temp files</th>'
            '<th rowspan="2" title="Database size as is was at the moment of last sample in report interval">Size</th>'
            '<th rowspan="2" title="Database size increment during report interval">Growth</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of transactions in this database that have been committed">Commits</th>'
            '<th title="Number of transactions in this database that have been rolled back">Rollbacks</th>'
            '<th title="Number of deadlocks detected in this database">Deadlocks</th>'
            '{checksum_fail_detected?checksum_fail_hdr2}'
            '<th title="Buffer cache hit ratio">Hit(%)</th>'
            '<th title="Number of disk blocks read in this database">Read</th>'
            '<th title="Number of times disk blocks were found already in the buffer cache">Hit</th>'
            '{io_times?io_times_hdr2}'
            '<th title="Number of rows returned by queries in this database">Ret</th>'
            '<th title="Number of rows fetched by queries in this database">Fet</th>'
            '<th title="Number of rows inserted by queries in this database">Ins</th>'
            '<th title="Number of rows updated by queries in this database">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Total amount of data written to temporary files by queries in this database">Size</th>'
            '<th title="Number of temporary files created by queries in this database">Files</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%1$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%2$s</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '{checksum_fail_detected?checksum_fail_row1}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '{io_times?io_times_row1}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%19$s</td>'
          '<td {value}>%20$s</td>'
          '<td {value}>%21$s</td>'
          '{checksum_fail_detected?checksum_fail_row2}'
          '<td {value}>%24$s</td>'
          '<td {value}>%25$s</td>'
          '<td {value}>%26$s</td>'
          '{io_times?io_times_row2}'
          '<td {value}>%27$s</td>'
          '<td {value}>%28$s</td>'
          '<td {value}>%29$s</td>'
          '<td {value}>%30$s</td>'
          '<td {value}>%31$s</td>'
          '<td {value}>%32$s</td>'
          '<td {value}>%33$s</td>'
          '<td {value}>%34$s</td>'
          '<td {value}>%35$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'checksum_fail_detected?checksum_fail_hdr1',
        '<th colspan="2">Checksums</th>',
      'checksum_fail_detected?checksum_fail_hdr2',
        '<th title="Number of block checksum failures detected">Failures</th>'
        '<th title="Last checksum filure detected">Last</th>',
      'checksum_fail_detected?checksum_fail_row1',
        '<td {value}><strong>%5$s</strong></td>'
        '<td {value}><strong>%6$s</strong></td>',
      'checksum_fail_detected?checksum_fail_row2',
        '<td {value}><strong>%22$s</strong></td>'
        '<td {value}><strong>%23$s</strong></td>',
      'io_times?io_times_hdr1',
        '<th colspan="2">Block I/O times</th>',
      'io_times?io_times_hdr2',
        '<th title="Time spent reading data file blocks by backends, in seconds">Read</th>'
        '<th title="Time spent writing data file blocks by backends, in seconds">Write</th>',
      'io_times?io_times_row1',
        '<td {value}>%36$s</td>'
        '<td {value}>%37$s</td>',
      'io_times?io_times_row2',
        '<td {value}>%38$s</td>'
        '<td {value}>%39$s</td>'
    );
    -- apply settings to templates

    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            r_result.xact_commit1,
            r_result.xact_rollback1,
            r_result.deadlocks1,
            r_result.checksum_failures1,
            r_result.checksum_last_failure1,
            round(CAST(r_result.blks_hit_pct1 AS numeric),2),
            r_result.blks_read1,
            r_result.blks_hit1,
            r_result.tup_returned1,
            r_result.tup_fetched1,
            r_result.tup_inserted1,
            r_result.tup_updated1,
            r_result.tup_deleted1,
            r_result.temp_bytes1,
            r_result.temp_files1,
            r_result.datsize1,
            r_result.datsize_delta1,
            r_result.xact_commit2,
            r_result.xact_rollback2,
            r_result.deadlocks2,
            r_result.checksum_failures2,
            r_result.checksum_last_failure2,
            round(CAST(r_result.blks_hit_pct2 AS numeric),2),
            r_result.blks_read2,
            r_result.blks_hit2,
            r_result.tup_returned2,
            r_result.tup_fetched2,
            r_result.tup_inserted2,
            r_result.tup_updated2,
            r_result.tup_deleted2,
            r_result.temp_bytes2,
            r_result.temp_files2,
            r_result.datsize2,
            r_result.datsize_delta2,
            round(CAST(r_result.blk_read_time1 AS numeric),2),
            round(CAST(r_result.blk_write_time1 AS numeric),2),
            round(CAST(r_result.blk_read_time2 AS numeric),2),
            round(CAST(r_result.blk_write_time2 AS numeric),2)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_sessions_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer, topn integer) FOR
    SELECT
        COALESCE(st.dbname,'Total') as dbname,
        NULLIF(sum(st.session_time), 0) as session_time,
        NULLIF(sum(st.active_time), 0) as active_time,
        NULLIF(sum(st.idle_in_transaction_time), 0) as idle_in_transaction_time,
        NULLIF(sum(st.sessions), 0) as sessions,
        NULLIF(sum(st.sessions_abandoned), 0) as sessions_abandoned,
        NULLIF(sum(st.sessions_fatal), 0) as sessions_fatal,
        NULLIF(sum(st.sessions_killed), 0) as sessions_killed
    FROM dbstats_sessions(sserver_id,start1_id,end1_id,topn) st
      LEFT OUTER JOIN sample_stat_database st_last ON
        (st_last.server_id = st.server_id AND st_last.datid = st.datid AND st_last.sample_id =
        end1_id)
    GROUP BY ROLLUP(st.dbname)
    ORDER BY st.dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th colspan="3" title="Session timings for databases">Timings (s)</th>'
            '<th colspan="4" title="Session counts for databases">Sessions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by database sessions in this database (note that statistics are only updated when the state of a session changes, so if sessions have been idle for a long time, this idle time won''t be included)">Total</th>'
            '<th title="Time spent executing SQL statements in this database (this corresponds to the states active and fastpath function call in pg_stat_activity)">Active</th>'
            '<th title="Time spent idling while in a transaction in this database (this corresponds to the states idle in transaction and idle in transaction (aborted) in pg_stat_activity)">Idle(T)</th>'
            '<th title="Total number of sessions established to this database">Established</th>'
            '<th title="Number of database sessions to this database that were terminated because connection to the client was lost">Abondoned</th>'
            '<th title="Number of database sessions to this database that were terminated by fatal errors">Fatal</th>'
            '<th title="Number of database sessions to this database that were terminated by operator intervention">Killed</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
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
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            round(CAST(r_result.session_time / 1000 AS numeric),2),
            round(CAST(r_result.active_time / 1000 AS numeric),2),
            round(CAST(r_result.idle_in_transaction_time / 1000 AS numeric),2),
            r_result.sessions,
            r_result.sessions_abandoned,
            r_result.sessions_fatal,
            r_result.sessions_killed
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_sessions_diff_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer)
    FOR
    SELECT
        COALESCE(COALESCE(dbs1.dbname,dbs2.dbname),'Total') AS dbname,
        NULLIF(sum(dbs1.session_time), 0) as session_time1,
        NULLIF(sum(dbs1.active_time), 0) as active_time1,
        NULLIF(sum(dbs1.idle_in_transaction_time), 0) as idle_in_transaction_time1,
        NULLIF(sum(dbs1.sessions), 0) as sessions1,
        NULLIF(sum(dbs1.sessions_abandoned), 0) as sessions_abandoned1,
        NULLIF(sum(dbs1.sessions_fatal), 0) as sessions_fatal1,
        NULLIF(sum(dbs1.sessions_killed), 0) as sessions_killed1,
        NULLIF(sum(dbs2.session_time), 0) as session_time2,
        NULLIF(sum(dbs2.active_time), 0) as active_time2,
        NULLIF(sum(dbs2.idle_in_transaction_time), 0) as idle_in_transaction_time2,
        NULLIF(sum(dbs2.sessions), 0) as sessions2,
        NULLIF(sum(dbs2.sessions_abandoned), 0) as sessions_abandoned2,
        NULLIF(sum(dbs2.sessions_fatal), 0) as sessions_fatal2,
        NULLIF(sum(dbs2.sessions_killed), 0) as sessions_killed2
    FROM dbstats_sessions(sserver_id,start1_id,end1_id,topn) dbs1
      FULL OUTER JOIN dbstats_sessions(sserver_id,start2_id,end2_id,topn) dbs2
        USING (server_id, datid)
      LEFT OUTER JOIN sample_stat_database st_last1 ON
        (st_last1.server_id = dbs1.server_id AND st_last1.datid = dbs1.datid AND st_last1.sample_id =
        end1_id)
      LEFT OUTER JOIN sample_stat_database st_last2 ON
        (st_last2.server_id = dbs2.server_id AND st_last2.datid = dbs2.datid AND st_last2.sample_id =
        end2_id)
    GROUP BY ROLLUP(COALESCE(dbs1.dbname,dbs2.dbname))
    ORDER BY COALESCE(dbs1.dbname,dbs2.dbname) NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="3" title="Session timings for databases">Timings (s)</th>'
            '<th colspan="4" title="Session counts for databases">Sessions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by database sessions in this database (note that statistics are only updated when the state of a session changes, so if sessions have been idle for a long time, this idle time won''t be included)">Total</th>'
            '<th title="Time spent executing SQL statements in this database (this corresponds to the states active and fastpath function call in pg_stat_activity)">Active</th>'
            '<th title="Time spent idling while in a transaction in this database (this corresponds to the states idle in transaction and idle in transaction (aborted) in pg_stat_activity)">Idle(T)</th>'
            '<th title="Total number of sessions established to this database">Established</th>'
            '<th title="Number of database sessions to this database that were terminated because connection to the client was lost">Abondoned</th>'
            '<th title="Number of database sessions to this database that were terminated by fatal errors">Fatal</th>'
            '<th title="Number of database sessions to this database that were terminated by operator intervention">Killed</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
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
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            round(CAST(r_result.session_time1 / 1000 AS numeric),2),
            round(CAST(r_result.active_time1 / 1000 AS numeric),2),
            round(CAST(r_result.idle_in_transaction_time1 / 1000 AS numeric),2),
            r_result.sessions1,
            r_result.sessions_abandoned1,
            r_result.sessions_fatal1,
            r_result.sessions_killed1,
            round(CAST(r_result.session_time2 / 1000 AS numeric),2),
            round(CAST(r_result.active_time2 / 1000 AS numeric),2),
            round(CAST(r_result.idle_in_transaction_time2 / 1000 AS numeric),2),
            r_result.sessions2,
            r_result.sessions_abandoned2,
            r_result.sessions_fatal2,
            r_result.sessions_killed2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION tbl_top_dead_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR (n_id integer, e_id integer, cnt integer) FOR
    SELECT
        sample_db.datname AS dbname,
        schemaname,
        relname,
        NULLIF(n_live_tup, 0) as n_live_tup,
        n_dead_tup as n_dead_tup,
        n_dead_tup * 100 / NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) AS dead_pct,
        last_autovacuum,
        COALESCE(
          pg_size_pretty(relsize),
          '['||pg_size_pretty(relpages_bytes)||']'
        ) AS relsize
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
    WHERE st.server_id=n_id AND NOT sample_db.datistemplate AND sample_id = e_id
        -- Min 5 MB in size
        AND COALESCE(st.relsize,st.relpages_bytes) > 5 * 1024^2
        AND st.n_dead_tup > 0
    ORDER BY n_dead_tup*100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) DESC,
      st.datid ASC, st.relid ASC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Estimated number of live rows">Live</th>'
            '<th title="Estimated number of dead rows">Dead</th>'
            '<th title="Dead rows count as a percentage of total rows count">%Dead</th>'
            '<th title="Last autovacuum ran time">Last AV</th>'
            '<th title="Table size without indexes and TOAST">Size</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(sserver_id,
      (report_context #>> '{report_properties,end1_id}')::integer,
      (report_context #>> '{report_properties,topn}')::integer)
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_live_tup,
            r_result.n_dead_tup,
            r_result.dead_pct,
            r_result.last_autovacuum,
            r_result.relsize
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tbl_top_mods_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR (n_id integer, e_id integer, cnt integer)
    FOR
    SELECT
        sample_db.datname AS dbname,
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze AS mods,
        n_mod_since_analyze*100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) AS mods_pct,
        last_autoanalyze,
        COALESCE(
          pg_size_pretty(relsize),
          '['||pg_size_pretty(relpages_bytes)||']'
        ) AS relsize
    FROM v_sample_stat_tables st
        -- Database name and existance condition
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
    WHERE st.server_id = n_id AND NOT sample_db.datistemplate AND sample_id = e_id
        AND st.relkind IN ('r','m')
        -- Min 5 MB in size
        AND COALESCE(st.relsize,st.relpages_bytes) > 5 * 1024^2
        AND n_mod_since_analyze > 0
        AND n_live_tup + n_dead_tup > 0
    ORDER BY n_mod_since_analyze*100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) DESC,
      st.datid ASC, st.relid ASC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Estimated number of live rows">Live</th>'
            '<th title="Estimated number of dead rows">Dead</th>'
            '<th title="Estimated number of rows modified since this table was last analyzed">Mod</th>'
            '<th title="Modified rows count as a percentage of total rows count">%Mod</th>'
            '<th title="Last autoanalyze ran time">Last AA</th>'
            '<th title="Table size without indexes and TOAST">Size</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(sserver_id,
      (report_context #>> '{report_properties,end1_id}')::integer,
      (report_context #>> '{report_properties,topn}')::integer)
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_live_tup,
            r_result.n_dead_tup,
            r_result.mods,
            r_result.mods_pct,
            r_result.last_autoanalyze,
            r_result.relsize
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
/* ===== Function stats functions ===== */
CREATE FUNCTION profile_checkavail_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(calls), 0) > 0
  FROM sample_stat_user_func_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_trg_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(calls), 0) > 0
  FROM sample_stat_user_func_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
    AND sn.trg_fn
$$ LANGUAGE sql;
/* ===== Function stats functions ===== */

CREATE FUNCTION top_functions(IN sserver_id integer, IN start_id integer, IN end_id integer, IN trigger_fn boolean)
RETURNS TABLE(
    server_id     integer,
    datid       oid,
    funcid      oid,
    dbname      name,
    schemaname  name,
    funcname    name,
    funcargs    text,
    calls       bigint,
    total_time  double precision,
    self_time   double precision,
    m_time      double precision,
    m_stime     double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.datid,
        st.funcid,
        sample_db.datname AS dbname,
        st.schemaname,
        st.funcname,
        st.funcargs,
        sum(st.calls)::bigint AS calls,
        sum(st.total_time)/1000 AS total_time,
        sum(st.self_time)/1000 AS self_time,
        sum(st.total_time)/NULLIF(sum(st.calls),0)/1000 AS m_time,
        sum(st.self_time)/NULLIF(sum(st.calls),0)/1000 AS m_stime
    FROM v_sample_stat_user_functions st
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
    WHERE
      st.server_id = sserver_id
      AND st.trg_fn = trigger_fn
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.funcid,sample_db.datname,st.schemaname,st.funcname,st.funcargs
$$ LANGUAGE sql;

CREATE FUNCTION func_top_time_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR (topn integer) FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions1
    WHERE total_time > 0
    ORDER BY
      total_time DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    FOR r_result IN c_fun_stats((report_context #>> '{report_properties,topn}')::integer) LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION func_top_time_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR (topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        COALESCE(f1.funcargs,f2.funcargs) as funcargs,
        NULLIF(f1.calls, 0) as calls1,
        NULLIF(f1.total_time, 0.0) as total_time1,
        NULLIF(f1.self_time, 0.0) as self_time1,
        NULLIF(f1.m_time, 0.0) as m_time1,
        NULLIF(f1.m_stime, 0.0) as m_stime1,
        NULLIF(f2.calls, 0) as calls2,
        NULLIF(f2.total_time, 0.0) as total_time2,
        NULLIF(f2.self_time, 0.0) as self_time2,
        NULLIF(f2.m_time, 0.0) as m_time2,
        NULLIF(f2.m_stime, 0.0) as m_stime2,
        row_number() OVER (ORDER BY f1.total_time DESC NULLS LAST) as rn_time1,
        row_number() OVER (ORDER BY f2.total_time DESC NULLS LAST) as rn_time2
    FROM top_functions1 f1
        FULL OUTER JOIN top_functions2 f2 USING (server_id, datid, funcid)
    ORDER BY
      COALESCE(f1.total_time, 0.0) + COALESCE(f2.total_time, 0.0) DESC,
      COALESCE(f1.datid,f2.datid) ASC,
      COALESCE(f1.funcid,f2.funcid) ASC
    ) t1
    WHERE COALESCE(total_time1, 0.0) + COALESCE(total_time2, 0.0) > 0.0
      AND least(
        rn_time1,
        rn_time2
      ) <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr} title="%s">%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    FOR r_result IN c_fun_stats((report_context #>> '{report_properties,topn}')::integer) LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            round(CAST(r_result.self_time1 AS numeric),2),
            round(CAST(r_result.m_time1 AS numeric),3),
            round(CAST(r_result.m_stime1 AS numeric),3),
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            round(CAST(r_result.self_time2 AS numeric),2),
            round(CAST(r_result.m_time2 AS numeric),3),
            round(CAST(r_result.m_stime2 AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION func_top_calls_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions1
    WHERE calls > 0
    ORDER BY
      calls DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    FOR r_result IN c_fun_stats((report_context #>> '{report_properties,topn}')::integer) LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

   IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
   ELSE
        RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION func_top_calls_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        COALESCE(f1.funcargs,f2.funcargs) as funcargs,
        NULLIF(f1.calls, 0) as calls1,
        NULLIF(f1.total_time, 0.0) as total_time1,
        NULLIF(f1.self_time, 0.0) as self_time1,
        NULLIF(f1.m_time, 0.0) as m_time1,
        NULLIF(f1.m_stime, 0.0) as m_stime1,
        NULLIF(f2.calls, 0) as calls2,
        NULLIF(f2.total_time, 0.0) as total_time2,
        NULLIF(f2.self_time, 0.0) as self_time2,
        NULLIF(f2.m_time, 0.0) as m_time2,
        NULLIF(f2.m_stime, 0.0) as m_stime2,
        row_number() OVER (ORDER BY f1.calls DESC NULLS LAST) as rn_calls1,
        row_number() OVER (ORDER BY f2.calls DESC NULLS LAST) as rn_calls2
    FROM top_functions1 f1
        FULL OUTER JOIN top_functions2 f2 USING (server_id, datid, funcid)
    ORDER BY
      COALESCE(f1.calls, 0) + COALESCE(f2.calls, 0) DESC,
      COALESCE(f1.datid,f2.datid) ASC,
      COALESCE(f1.funcid,f2.funcid) ASC
    ) t1
    WHERE COALESCE(calls1, 0) + COALESCE(calls2, 0) > 0
      AND least(
        rn_calls1,
        rn_calls2
      ) <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr} title="%s">%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    FOR r_result IN c_fun_stats((report_context #>> '{report_properties,topn}')::integer) LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            round(CAST(r_result.self_time1 AS numeric),2),
            round(CAST(r_result.m_time1 AS numeric),3),
            round(CAST(r_result.m_stime1 AS numeric),3),
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            round(CAST(r_result.self_time2 AS numeric),2),
            round(CAST(r_result.m_time2 AS numeric),3),
            round(CAST(r_result.m_stime2 AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

/* ==== Trigger report functions ==== */

CREATE FUNCTION func_top_trg_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR(start_id integer, end_id integer, topn integer) FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions(sserver_id, start_id, end_id, true)
    WHERE total_time > 0
    ORDER BY
      total_time DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    FOR r_result IN c_fun_stats(
      (report_context #>> '{report_properties,start1_id}')::integer,
      (report_context #>> '{report_properties,end1_id}')::integer,
      (report_context #>> '{report_properties,topn}')::integer)
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

   IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
   ELSE
        RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION func_top_trg_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        COALESCE(f1.funcargs,f2.funcargs) as funcargs,
        NULLIF(f1.calls, 0) as calls1,
        NULLIF(f1.total_time, 0.0) as total_time1,
        NULLIF(f1.self_time, 0.0) as self_time1,
        NULLIF(f1.m_time, 0.0) as m_time1,
        NULLIF(f1.m_stime, 0.0) as m_stime1,
        NULLIF(f2.calls, 0) as calls2,
        NULLIF(f2.total_time, 0.0) as total_time2,
        NULLIF(f2.self_time, 0.0) as self_time2,
        NULLIF(f2.m_time, 0.0) as m_time2,
        NULLIF(f2.m_stime, 0.0) as m_stime2,
        row_number() OVER (ORDER BY f1.total_time DESC NULLS LAST) as rn_time1,
        row_number() OVER (ORDER BY f2.total_time DESC NULLS LAST) as rn_time2
    FROM top_functions(sserver_id, start1_id, end1_id, true) f1
        FULL OUTER JOIN top_functions(sserver_id, start2_id, end2_id, true) f2 USING (server_id, datid, funcid)
    ORDER BY
      COALESCE(f1.total_time, 0.0) + COALESCE(f2.total_time, 0.0) DESC,
      COALESCE(f1.datid,f2.datid) ASC,
      COALESCE(f1.funcid,f2.funcid) ASC
    ) t1
    WHERE COALESCE(total_time1, 0.0) + COALESCE(total_time2, 0.0) > 0.0
      AND least(
        rn_time1,
        rn_time2
      ) <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr} title="%s">%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    FOR r_result IN c_fun_stats(
      (report_context #>> '{report_properties,start1_id}')::integer,
      (report_context #>> '{report_properties,end1_id}')::integer,
      (report_context #>> '{report_properties,start2_id}')::integer,
      (report_context #>> '{report_properties,end2_id}')::integer,
      (report_context #>> '{report_properties,topn}')::integer)
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            round(CAST(r_result.self_time1 AS numeric),2),
            round(CAST(r_result.m_time1 AS numeric),3),
            round(CAST(r_result.m_stime1 AS numeric),3),
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            round(CAST(r_result.self_time2 AS numeric),2),
            round(CAST(r_result.m_time2 AS numeric),3),
            round(CAST(r_result.m_stime2 AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
/* ===== Indexes stats functions ===== */

CREATE FUNCTION top_indexes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    datid               oid,
    relid               oid,
    indexrelid          oid,
    indisunique         boolean,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelname        name,
    idx_scan            bigint,
    growth              bigint,
    tbl_n_tup_ins       bigint,
    tbl_n_tup_upd       bigint,
    tbl_n_tup_del       bigint,
    tbl_n_tup_hot_upd   bigint,
    relpagegrowth_bytes bigint,
    idx_blks_read       bigint,
    idx_blks_fetch      bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        st.indexrelid,
        st.indisunique,
        sample_db.datname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name as relname,
        st.indexrelname,
        sum(st.idx_scan)::bigint as idx_scan,
        sum(st.relsize_diff)::bigint as growth,
        sum(tbl.n_tup_ins)::bigint as tbl_n_tup_ins,
        sum(tbl.n_tup_upd)::bigint as tbl_n_tup_upd,
        sum(tbl.n_tup_del)::bigint as tbl_n_tup_del,
        sum(tbl.n_tup_hot_upd)::bigint as tbl_n_tup_hot_upd,
        sum(st.relpages_bytes_diff)::bigint as relpagegrowth_bytes,
        sum(st.idx_blks_read)::bigint as idx_blks_read,
        sum(st.idx_blks_hit)::bigint + sum(st.idx_blks_read)::bigint as idx_blks_fetch
    FROM v_sample_stat_indexes st JOIN sample_stat_tables tbl USING (server_id, sample_id, datid, relid)
        -- Database name
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
        JOIN tablespaces_list ON (st.server_id, st.tablespaceid) = (tablespaces_list.server_id, tablespaces_list.tablespaceid)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON
          (mtbl.server_id, mtbl.datid, mtbl.reltoastrelid) =
          (st.server_id, st.datid, st.relid)
    WHERE st.server_id=sserver_id AND NOT sample_db.datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,st.indexrelid,st.indisunique,sample_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname),COALESCE(mtbl.relname||'(TOAST)',st.relname), tablespaces_list.tablespacename,st.indexrelname
$$ LANGUAGE sql;

CREATE FUNCTION top_growth_indexes_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Indexes stats template
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(end1_id integer, topn integer) FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        NULLIF(st.best_growth, 0) as growth,
        NULLIF(st_last.relsize, 0) as relsize,
        NULLIF(st_last.relpages_bytes, 0) as relpages_bytes,
        NULLIF(tbl_n_tup_ins, 0) as tbl_n_tup_ins,
        NULLIF(tbl_n_tup_upd - COALESCE(tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd,
        NULLIF(tbl_n_tup_del, 0) as tbl_n_tup_del,
        st.relsize_growth_avail
    FROM top_indexes1 st
        JOIN sample_stat_indexes st_last USING (server_id,datid,indexrelid)
    WHERE st_last.sample_id = end1_id
      AND st.best_growth > 0
    ORDER BY st.best_growth DESC,
      COALESCE(tbl_n_tup_ins,0) + COALESCE(tbl_n_tup_upd,0) + COALESCE(tbl_n_tup_del,0) DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            COALESCE(
              pg_size_pretty(r_result.relsize),
              '['||pg_size_pretty(r_result.relpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail
              THEN pg_size_pretty(r_result.growth)
              ELSE '['||pg_size_pretty(r_result.growth)||']'
            END,
            r_result.tbl_n_tup_ins,
            r_result.tbl_n_tup_upd,
            r_result.tbl_n_tup_del
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_growth_indexes_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(end1_id integer, end2_id integer, topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(ix1.dbname,ix2.dbname) as dbname,
        COALESCE(ix1.tablespacename,ix2.tablespacename) as tablespacename,
        COALESCE(ix1.schemaname,ix2.schemaname) as schemaname,
        COALESCE(ix1.relname,ix2.relname) as relname,
        COALESCE(ix1.indexrelname,ix2.indexrelname) as indexrelname,
        NULLIF(ix1.best_growth, 0) as growth1,
        NULLIF(ix_last1.relsize, 0) as relsize1,
        NULLIF(ix_last1.relpages_bytes, 0) as relpages_bytes1,
        NULLIF(ix1.tbl_n_tup_ins, 0) as tbl_n_tup_ins1,
        NULLIF(ix1.tbl_n_tup_upd - COALESCE(ix1.tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd1,
        NULLIF(ix1.tbl_n_tup_del, 0) as tbl_n_tup_del1,
        NULLIF(ix2.best_growth, 0) as growth2,
        NULLIF(ix_last2.relsize, 0) as relsize2,
        NULLIF(ix_last2.relpages_bytes, 0) as relpages_bytes2,
        NULLIF(ix2.tbl_n_tup_ins, 0) as tbl_n_tup_ins2,
        NULLIF(ix2.tbl_n_tup_upd - COALESCE(ix2.tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd2,
        NULLIF(ix2.tbl_n_tup_del, 0) as tbl_n_tup_del2,
        ix1.relsize_growth_avail as relsize_growth_avail1,
        ix2.relsize_growth_avail as relsize_growth_avail2,
        row_number() over (ORDER BY ix1.best_growth DESC NULLS LAST) as rn_growth1,
        row_number() over (ORDER BY ix2.best_growth DESC NULLS LAST) as rn_growth2
    FROM top_indexes1 ix1
        FULL OUTER JOIN top_indexes2 ix2 USING (server_id, datid, indexrelid)
        LEFT OUTER JOIN sample_stat_indexes ix_last1 ON
          (ix_last1.sample_id, ix_last1.server_id, ix_last1.datid, ix_last1.indexrelid) =
          (end1_id, ix1.server_id, ix1.datid, ix1.indexrelid)
        LEFT OUTER JOIN sample_stat_indexes ix_last2 ON
          (ix_last2.sample_id, ix_last2.server_id, ix_last2.datid, ix_last2.indexrelid) =
          (end2_id, ix2.server_id, ix2.datid, ix2.indexrelid)
    WHERE COALESCE(ix1.best_growth, 0) + COALESCE(ix2.best_growth, 0) > 0
    ORDER BY COALESCE(ix1.best_growth, 0) + COALESCE(ix2.best_growth, 0) DESC,
      COALESCE(ix1.tbl_n_tup_ins,0) + COALESCE(ix1.tbl_n_tup_upd,0) + COALESCE(ix1.tbl_n_tup_del,0) +
      COALESCE(ix2.tbl_n_tup_ins,0) + COALESCE(ix2.tbl_n_tup_upd,0) + COALESCE(ix2.tbl_n_tup_del,0) DESC,
      COALESCE(ix1.datid,ix2.datid) ASC,
      COALESCE(ix1.relid,ix2.relid) ASC,
      COALESCE(ix1.indexrelid,ix2.indexrelid) ASC
    ) t1
    WHERE least(
        rn_growth1,
        rn_growth2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            COALESCE(
              pg_size_pretty(r_result.relsize1),
              '['||pg_size_pretty(r_result.relpages_bytes1)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail1
              THEN pg_size_pretty(r_result.growth1)
              ELSE '['||pg_size_pretty(r_result.growth1)||']'
            END,
            r_result.tbl_n_tup_ins1,
            r_result.tbl_n_tup_upd1,
            r_result.tbl_n_tup_del1,
            COALESCE(
              pg_size_pretty(r_result.relsize2),
              '['||pg_size_pretty(r_result.relpages_bytes2)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail2
              THEN pg_size_pretty(r_result.growth2)
              ELSE '['||pg_size_pretty(r_result.growth2)||']'
            END,
            r_result.tbl_n_tup_ins2,
            r_result.tbl_n_tup_upd2,
            r_result.tbl_n_tup_del2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_unused_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(end1_id integer, topn integer) FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        NULLIF(st.best_growth, 0) as growth,
        NULLIF(st_last.relsize, 0) as relsize,
        NULLIF(st_last.relpages_bytes, 0) as relpages_bytes,
        NULLIF(tbl_n_tup_ins, 0) as tbl_n_tup_ins,
        NULLIF(tbl_n_tup_upd - COALESCE(tbl_n_tup_hot_upd,0), 0) as tbl_n_ind_upd,
        NULLIF(tbl_n_tup_del, 0) as tbl_n_tup_del,
        st.relsize_growth_avail
    FROM top_indexes1 st
        JOIN sample_stat_indexes st_last using (server_id,datid,indexrelid)
    WHERE st_last.sample_id=end1_id
      AND COALESCE(st.idx_scan, 0) = 0 AND NOT st.indisunique
      AND COALESCE(tbl_n_tup_ins, 0) + COALESCE(tbl_n_tup_upd, 0) + COALESCE(tbl_n_tup_del, 0) > 0
    ORDER BY
      COALESCE(tbl_n_tup_ins, 0) + COALESCE(tbl_n_tup_upd, 0) + COALESCE(tbl_n_tup_del, 0) DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespaces</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_ix_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            COALESCE(
              pg_size_pretty(r_result.relsize),
              '['||pg_size_pretty(r_result.relpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail
              THEN pg_size_pretty(r_result.growth)
              ELSE '['||pg_size_pretty(r_result.growth)||']'
            END,
            r_result.tbl_n_tup_ins,
            r_result.tbl_n_ind_upd,
            r_result.tbl_n_tup_del
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION top_vacuumed_indexes_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Indexes stats template
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(start1_id integer, end1_id integer, topn integer) FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        NULLIF(vac.vacuum_count, 0) as vacuum_count,
        NULLIF(vac.autovacuum_count, 0) as autovacuum_count,
        NULLIF(vac.vacuum_bytes, 0) as vacuum_bytes,
        NULLIF(vac.avg_indexrelsize, 0) as avg_ix_relsize,
        NULLIF(vac.avg_relsize, 0) as avg_relsize,
        NULLIF(vac.relpages_vacuum_bytes, 0) as relpages_vacuum_bytes,
        NULLIF(vac.avg_indexrelpages_bytes, 0) as avg_indexrelpages_bytes,
        NULLIF(vac.avg_relpages_bytes, 0) as avg_relpages_bytes,
        vac.relsize_collected as relsize_collected
    FROM top_indexes1 st
      JOIN (
        SELECT
          server_id,
          datid,
          indexrelid,
          sum(vacuum_count) as vacuum_count,
          sum(autovacuum_count) as autovacuum_count,
          round(sum(i.relsize
      * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as vacuum_bytes,
          round(
            avg(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          )::bigint as avg_indexrelsize,
          round(
            avg(t.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          )::bigint as avg_relsize,

          round(sum(i.relpages_bytes
      * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as relpages_vacuum_bytes,
          round(
            avg(i.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          )::bigint as avg_indexrelpages_bytes,
          round(
            avg(t.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          )::bigint as avg_relpages_bytes,
          count(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0) =
          count(*) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
            as relsize_collected
        FROM sample_stat_indexes i
      JOIN indexes_list il USING (server_id,datid,indexrelid)
      JOIN sample_stat_tables t USING
        (server_id, sample_id, datid, relid)
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start1_id + 1 AND end1_id
        GROUP BY
          server_id, datid, indexrelid
      ) vac USING (server_id, datid, indexrelid)
    WHERE COALESCE(vac.vacuum_count, 0) + COALESCE(vac.autovacuum_count, 0) > 0
    ORDER BY CASE WHEN relsize_collected THEN vacuum_bytes ELSE relpages_vacuum_bytes END DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th title="Estimated implicit vacuum load caused by table indexes">~Vacuum bytes</th>'
            '<th title="Vacuum count on underlying table">Vacuum cnt</th>'
            '<th title="Autovacuum count on underlying table">Autovacuum cnt</th>'
            '<th title="Average index size during report interval">IX size</th>'
            '<th title="Average relation size during report interval">Relsize</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            CASE WHEN r_result.relsize_collected THEN
              pg_size_pretty(r_result.vacuum_bytes)
            ELSE
              '['||pg_size_pretty(r_result.relpages_vacuum_bytes)||']'
            END,
            r_result.vacuum_count,
            r_result.autovacuum_count,
            CASE WHEN r_result.relsize_collected THEN
              pg_size_pretty(r_result.avg_ix_relsize)
            ELSE
              '['||pg_size_pretty(r_result.avg_indexrelpages_bytes)||']'
            END,
            CASE WHEN r_result.relsize_collected THEN
              pg_size_pretty(r_result.avg_relsize)
            ELSE
              '['||pg_size_pretty(r_result.avg_relpages_bytes)||']'
            END
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_vacuumed_indexes_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer)
    FOR
    SELECT * FROM (SELECT
        COALESCE(ix1.dbname,ix2.dbname) as dbname,
        COALESCE(ix1.tablespacename,ix2.tablespacename) as tablespacename,
        COALESCE(ix1.schemaname,ix2.schemaname) as schemaname,
        COALESCE(ix1.relname,ix2.relname) as relname,
        COALESCE(ix1.indexrelname,ix2.indexrelname) as indexrelname,
        NULLIF(vac1.vacuum_count, 0) as vacuum_count1,
        NULLIF(vac1.autovacuum_count, 0) as autovacuum_count1,
        CASE WHEN vac1.relsize_collected THEN
          NULLIF(vac1.vacuum_bytes, 0)
        ELSE
          NULLIF(vac1.relpages_vacuum_bytes, 0)
        END as best_vacuum_bytes1,
--        NULLIF(vac1.vacuum_bytes, 0) as vacuum_bytes1,
        NULLIF(vac1.avg_indexrelsize, 0) as avg_ix_relsize1,
        NULLIF(vac1.avg_relsize, 0) as avg_relsize1,
--        NULLIF(vac1.relpages_vacuum_bytes, 0) as relpages_vacuum_bytes1,
        NULLIF(vac1.avg_indexrelpages_bytes, 0) as avg_indexrelpages_bytes1,
        NULLIF(vac1.avg_relpages_bytes, 0) as avg_relpages_bytes1,
        vac1.relsize_collected as relsize_collected1,
        NULLIF(vac2.vacuum_count, 0) as vacuum_count2,
        NULLIF(vac2.autovacuum_count, 0) as autovacuum_count2,
        CASE WHEN vac2.relsize_collected THEN
          NULLIF(vac2.vacuum_bytes, 0)
        ELSE
          NULLIF(vac2.relpages_vacuum_bytes, 0)
        END as best_vacuum_bytes2,
--        NULLIF(vac2.vacuum_bytes, 0) as vacuum_bytes2,
        NULLIF(vac2.avg_indexrelsize, 0) as avg_ix_relsize2,
        NULLIF(vac2.avg_relsize, 0) as avg_relsize2,
        --NULLIF(vac2.relpages_vacuum_bytes, 0) as relpages_vacuum_bytes2,
        NULLIF(vac2.avg_indexrelpages_bytes, 0) as avg_indexrelpages_bytes2,
        NULLIF(vac2.avg_relpages_bytes, 0) as avg_relpages_bytes2,
        vac2.relsize_collected as relsize_collected2,
        row_number() over (ORDER BY
          CASE WHEN vac1.relsize_collected THEN vac1.vacuum_bytes ELSE vac1.relpages_vacuum_bytes END
          DESC NULLS LAST)
          as rn_vacuum_bytes1,
        row_number() over (ORDER BY
          CASE WHEN vac2.relsize_collected THEN vac2.vacuum_bytes ELSE vac2.relpages_vacuum_bytes END
          DESC NULLS LAST)
          as rn_vacuum_bytes2
    FROM top_indexes1 ix1
        FULL OUTER JOIN top_indexes2 ix2 USING (server_id, datid, indexrelid)
        -- Join interpolated data of interval 1
        LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        indexrelid,
        sum(vacuum_count) as vacuum_count,
        sum(autovacuum_count) as autovacuum_count,
        round(sum(i.relsize
          * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as vacuum_bytes,
        round(avg(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_indexrelsize,
        round(avg(t.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_relsize,
        round(sum(i.relpages_bytes
          * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as relpages_vacuum_bytes,
        round(avg(i.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_indexrelpages_bytes,
        round(avg(t.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_relpages_bytes,
        count(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0) =
        count(*) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          as relsize_collected
      FROM sample_stat_indexes i
        JOIN indexes_list il USING (server_id,datid,indexrelid)
        JOIN sample_stat_tables t USING
          (server_id, sample_id, datid, relid)
      WHERE
        server_id = sserver_id AND
        sample_id BETWEEN start1_id + 1 AND end1_id
      GROUP BY
        server_id, datid, indexrelid
        ) vac1 USING (server_id, datid, indexrelid)
        -- Join interpolated data of interval 2
        LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        indexrelid,
        sum(vacuum_count) as vacuum_count,
        sum(autovacuum_count) as autovacuum_count,
        round(sum(i.relsize
          * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as vacuum_bytes,
        round(avg(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_indexrelsize,
        round(avg(t.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_relsize,
        round(sum(i.relpages_bytes
          * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as relpages_vacuum_bytes,
        round(avg(i.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_indexrelpages_bytes,
        round(avg(t.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_relpages_bytes,
        count(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0) =
        count(*) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          as relsize_collected
      FROM sample_stat_indexes i
        JOIN indexes_list il USING (server_id,datid,indexrelid)
        JOIN sample_stat_tables t USING
          (server_id, sample_id, datid, relid)
      WHERE
        server_id = sserver_id AND
        sample_id BETWEEN start2_id + 1 AND end2_id
      GROUP BY
        server_id, datid, indexrelid
        ) vac2 USING (server_id, datid, indexrelid)
    WHERE COALESCE(vac1.vacuum_count, 0) + COALESCE(vac1.autovacuum_count, 0) +
        COALESCE(vac2.vacuum_count, 0) + COALESCE(vac2.autovacuum_count, 0) > 0
    ) t1
    WHERE least(
        rn_vacuum_bytes1,
        rn_vacuum_bytes2
      ) <= topn
    ORDER BY
      COALESCE(best_vacuum_bytes1, 0) + COALESCE(best_vacuum_bytes2, 0) DESC,
      dbname ASC,
      schemaname ASC,
      indexrelname ASC
    ;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th>I</th>'
            '<th title="Estimated implicit vacuum load caused by table indexes">~Vacuum bytes</th>'
            '<th title="Vacuum count on underlying table">Vacuum cnt</th>'
            '<th title="Autovacuum count on underlying table">Autovacuum cnt</th>'
            '<th title="Average index size during report interval">IX size</th>'
            '<th title="Average relation size during report interval">Relsize</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            CASE WHEN r_result.relsize_collected1 THEN
              pg_size_pretty(r_result.best_vacuum_bytes1)
            ELSE
              '['||pg_size_pretty(r_result.best_vacuum_bytes1)||']'
            END,
            r_result.vacuum_count1,
            r_result.autovacuum_count1,
            CASE WHEN r_result.relsize_collected1 THEN
              pg_size_pretty(r_result.avg_ix_relsize1)
            ELSE
              '['||pg_size_pretty(r_result.avg_indexrelpages_bytes1)||']'
            END,
            CASE WHEN r_result.relsize_collected1 THEN
              pg_size_pretty(r_result.avg_relsize1)
            ELSE
              '['||pg_size_pretty(r_result.avg_relpages_bytes1)||']'
            END,
            CASE WHEN r_result.relsize_collected2 THEN
              pg_size_pretty(r_result.best_vacuum_bytes2)
            ELSE
              '['||pg_size_pretty(r_result.best_vacuum_bytes2)||']'
            END,
            r_result.vacuum_count2,
            r_result.autovacuum_count2,
            CASE WHEN r_result.relsize_collected2 THEN
              pg_size_pretty(r_result.avg_ix_relsize2)
            ELSE
              '['||pg_size_pretty(r_result.avg_indexrelpages_bytes2)||']'
            END,
            CASE WHEN r_result.relsize_collected2 THEN
              pg_size_pretty(r_result.avg_relsize2)
            ELSE
              '['||pg_size_pretty(r_result.avg_relpages_bytes2)||']'
            END
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
/* ========= kcache stats functions ========= */

CREATE FUNCTION profile_checkavail_rusage(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
  SELECT
    count(*) = end_id - start_id
  FROM
    (SELECT
      sum(exec_user_time) > 0 as exec
    FROM sample_kcache_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY server_id, sample_id) exec_time_samples
  WHERE exec_time_samples.exec
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_rusage_planstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
  SELECT
    count(*) = end_id - start_id
  FROM
    (SELECT
      sum(plan_user_time) > 0 as plan
    FROM sample_kcache_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY server_id, sample_id) plan_time_samples
  WHERE plan_time_samples.plan
$$ LANGUAGE sql;
/* ===== Statements stats functions ===== */

CREATE FUNCTION top_kcache_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id                integer,
    datid                    oid,
    dbname                   name,
    userid                   oid,
    username                 name,
    queryid                  bigint,
    toplevel                 boolean,
    exec_user_time           double precision, --  User CPU time used
    user_time_pct            float, --  User CPU time used percentage
    exec_system_time         double precision, --  System CPU time used
    system_time_pct          float, --  System CPU time used percentage
    exec_minflts             bigint, -- Number of page reclaims (soft page faults)
    exec_majflts             bigint, -- Number of page faults (hard page faults)
    exec_nswaps              bigint, -- Number of swaps
    exec_reads               bigint, -- Number of bytes read by the filesystem layer
    exec_writes              bigint, -- Number of bytes written by the filesystem layer
    exec_msgsnds             bigint, -- Number of IPC messages sent
    exec_msgrcvs             bigint, -- Number of IPC messages received
    exec_nsignals            bigint, -- Number of signals received
    exec_nvcsws              bigint, -- Number of voluntary context switches
    exec_nivcsws             bigint,
    reads_total_pct          float,
    writes_total_pct         float,
    plan_user_time           double precision, --  User CPU time used
    plan_system_time         double precision, --  System CPU time used
    plan_minflts             bigint, -- Number of page reclaims (soft page faults)
    plan_majflts             bigint, -- Number of page faults (hard page faults)
    plan_nswaps              bigint, -- Number of swaps
    plan_reads               bigint, -- Number of bytes read by the filesystem layer
    plan_writes              bigint, -- Number of bytes written by the filesystem layer
    plan_msgsnds             bigint, -- Number of IPC messages sent
    plan_msgrcvs             bigint, -- Number of IPC messages received
    plan_nsignals            bigint, -- Number of signals received
    plan_nvcsws              bigint, -- Number of voluntary context switches
    plan_nivcsws             bigint
) SET search_path=@extschema@ AS $$
  WITH tot AS (
        SELECT
            COALESCE(sum(exec_user_time), 0.0) + COALESCE(sum(plan_user_time), 0.0) AS user_time,
            COALESCE(sum(exec_system_time), 0.0) + COALESCE(sum(plan_system_time), 0.0)  AS system_time,
            COALESCE(sum(exec_reads), 0) + COALESCE(sum(plan_reads), 0) AS reads,
            COALESCE(sum(exec_writes), 0) + COALESCE(sum(plan_writes), 0) AS writes
        FROM sample_kcache_total
        WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id)
    SELECT
        kc.server_id as server_id,
        kc.datid as datid,
        sample_db.datname as dbname,
        kc.userid as userid,
        rl.username as username,
        kc.queryid as queryid,
        kc.toplevel as toplevel,
        sum(kc.exec_user_time) as exec_user_time,
        ((COALESCE(sum(kc.exec_user_time), 0.0) + COALESCE(sum(kc.plan_user_time), 0.0))
          *100/NULLIF(min(tot.user_time),0.0))::float AS user_time_pct,
        sum(kc.exec_system_time) as exec_system_time,
        ((COALESCE(sum(kc.exec_system_time), 0.0) + COALESCE(sum(kc.plan_system_time), 0.0))
          *100/NULLIF(min(tot.system_time), 0.0))::float AS system_time_pct,
        sum(kc.exec_minflts)::bigint as exec_minflts,
        sum(kc.exec_majflts)::bigint as exec_majflts,
        sum(kc.exec_nswaps)::bigint as exec_nswaps,
        sum(kc.exec_reads)::bigint as exec_reads,
        sum(kc.exec_writes)::bigint as exec_writes,
        sum(kc.exec_msgsnds)::bigint as exec_msgsnds,
        sum(kc.exec_msgrcvs)::bigint as exec_msgrcvs,
        sum(kc.exec_nsignals)::bigint as exec_nsignals,
        sum(kc.exec_nvcsws)::bigint as exec_nvcsws,
        sum(kc.exec_nivcsws)::bigint as exec_nivcsws,
        ((COALESCE(sum(kc.exec_reads), 0) + COALESCE(sum(kc.plan_reads), 0))
          *100/NULLIF(min(tot.reads),0))::float AS reads_total_pct,
        ((COALESCE(sum(kc.exec_writes), 0) + COALESCE(sum(kc.plan_writes), 0))
          *100/NULLIF(min(tot.writes),0))::float AS writes_total_pct,
        sum(kc.plan_user_time) as plan_user_time,
        sum(kc.plan_system_time) as plan_system_time,
        sum(kc.plan_minflts)::bigint as plan_minflts,
        sum(kc.plan_majflts)::bigint as plan_majflts,
        sum(kc.plan_nswaps)::bigint as plan_nswaps,
        sum(kc.plan_reads)::bigint as plan_reads,
        sum(kc.plan_writes)::bigint as plan_writes,
        sum(kc.plan_msgsnds)::bigint as plan_msgsnds,
        sum(kc.plan_msgrcvs)::bigint as plan_msgrcvs,
        sum(kc.plan_nsignals)::bigint as plan_nsignals,
        sum(kc.plan_nvcsws)::bigint as plan_nvcsws,
        sum(kc.plan_nivcsws)::bigint as plan_nivcsws
   FROM sample_kcache kc
        -- User name
        JOIN roles_list rl USING (server_id, userid)
        -- Database name
        JOIN sample_stat_database sample_db
        USING (server_id, sample_id, datid)
        -- Total stats
        CROSS JOIN tot
    WHERE kc.server_id = sserver_id AND kc.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY
      kc.server_id,
      kc.datid,
      sample_db.datname,
      kc.userid,
      rl.username,
      kc.queryid,
      kc.toplevel
$$ LANGUAGE sql;


CREATE FUNCTION top_cpu_time_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR(topn integer) FOR
    SELECT
        kc.datid as datid,
        kc.dbname as dbname,
        kc.userid as userid,
        kc.username as username,
        kc.queryid as queryid,
        kc.toplevel as toplevel,
        NULLIF(kc.plan_user_time, 0.0) as plan_user_time,
        NULLIF(kc.exec_user_time, 0.0) as exec_user_time,
        NULLIF(kc.user_time_pct, 0.0) as user_time_pct,
        NULLIF(kc.plan_system_time, 0.0) as plan_system_time,
        NULLIF(kc.exec_system_time, 0.0) as exec_system_time,
        NULLIF(kc.system_time_pct, 0.0) as system_time_pct
    FROM top_kcache_statements1 kc
    WHERE COALESCE(kc.plan_user_time, 0.0) + COALESCE(kc.plan_system_time, 0.0) +
      COALESCE(kc.exec_user_time, 0.0) + COALESCE(kc.exec_system_time, 0.0) > 0
    ORDER BY COALESCE(kc.plan_user_time, 0.0) + COALESCE(kc.plan_system_time, 0.0) +
      COALESCE(kc.exec_user_time, 0.0) + COALESCE(kc.exec_system_time, 0.0) DESC,
      kc.datid,
      kc.userid,
      kc.queryid,
      kc.toplevel
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
          '<th title="Userspace CPU" colspan="{rusage_planstats?cputime_colspan}">User Time</th>'
          '<th title="Kernelspace CPU" colspan="{rusage_planstats?cputime_colspan}">System Time</th>'
        '</tr>'
        '<tr>'
          '{rusage_planstats?user_plan_time_hdr}'
          '<th title="User CPU time elapsed during execution">Exec (s)</th>'
          '<th title="User CPU time elapsed by this statement as a percentage of total user CPU time">%Total</th>'
          '{rusage_planstats?system_plan_time_hdr}'
          '<th title="System CPU time elapsed during execution">Exec (s)</th>'
          '<th title="System CPU time elapsed by this statement as a percentage of total system CPU time">%Total</th>'
        '</tr>'
        '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%5$s</td>'
          '{rusage_planstats?user_plan_time_row}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{rusage_planstats?system_plan_time_row}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'rusage_planstats?cputime_colspan','3',
      '!rusage_planstats?cputime_colspan','2',
      'rusage_planstats?user_plan_time_hdr',
        '<th title="User CPU time elapsed during planning">Plan (s)</th>',
      'rusage_planstats?system_plan_time_hdr',
        '<th title="System CPU time elapsed during planning">Plan (s)</th>',
      'rusage_planstats?user_plan_time_row',
        '<td {value}>%6$s</td>',
      'rusage_planstats?system_plan_time_row',
        '<td {value}>%9$s</td>'
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
            round(CAST(r_result.plan_user_time AS numeric),2),
            round(CAST(r_result.exec_user_time AS numeric),2),
            round(CAST(r_result.user_time_pct AS numeric),2),
            round(CAST(r_result.plan_system_time AS numeric),2),
            round(CAST(r_result.exec_system_time AS numeric),2),
            round(CAST(r_result.system_time_pct AS numeric),2)
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

CREATE FUNCTION top_cpu_time_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(kc1.datid,kc2.datid) as datid,
        COALESCE(kc1.dbname,kc2.dbname) as dbname,
        COALESCE(kc1.userid,kc2.userid) as userid,
        COALESCE(kc1.username,kc2.username) as username,
        COALESCE(kc1.queryid,kc2.queryid) as queryid,
        COALESCE(kc1.toplevel,kc2.toplevel) as toplevel,
        NULLIF(kc1.plan_user_time, 0.0) as plan_user_time1,
        NULLIF(kc1.exec_user_time, 0.0) as exec_user_time1,
        NULLIF(kc1.user_time_pct, 0.0) as user_time_pct1,
        NULLIF(kc1.plan_system_time, 0.0) as plan_system_time1,
        NULLIF(kc1.exec_system_time, 0.0) as exec_system_time1,
        NULLIF(kc1.system_time_pct, 0.0) as system_time_pct1,
        NULLIF(kc2.plan_user_time, 0.0) as plan_user_time2,
        NULLIF(kc2.exec_user_time, 0.0) as exec_user_time2,
        NULLIF(kc2.user_time_pct, 0.0) as user_time_pct2,
        NULLIF(kc2.plan_system_time, 0.0) as plan_system_time2,
        NULLIF(kc2.exec_system_time, 0.0) as exec_system_time2,
        NULLIF(kc2.system_time_pct, 0.0) as system_time_pct2,
        row_number() over (ORDER BY COALESCE(kc1.exec_user_time, 0.0) + COALESCE(kc1.exec_system_time, 0.0) DESC NULLS LAST) as time1,
        row_number() over (ORDER BY COALESCE(kc2.exec_user_time, 0.0) + COALESCE(kc2.exec_system_time, 0.0) DESC NULLS LAST) as time2
    FROM top_kcache_statements1 kc1
        FULL OUTER JOIN top_kcache_statements2 kc2 USING (server_id, datid, userid, queryid)
    WHERE COALESCE(kc1.plan_user_time, 0.0) + COALESCE(kc2.plan_user_time, 0.0) +
        COALESCE(kc1.plan_system_time, 0.0) + COALESCE(kc2.plan_system_time, 0.0) +
        COALESCE(kc1.exec_user_time, 0.0) + COALESCE(kc2.exec_user_time, 0.0) +
        COALESCE(kc1.exec_system_time, 0.0) + COALESCE(kc2.exec_system_time, 0.0) > 0
    ORDER BY COALESCE(kc1.plan_user_time, 0.0) + COALESCE(kc2.plan_user_time, 0.0) +
        COALESCE(kc1.plan_system_time, 0.0) + COALESCE(kc2.plan_system_time, 0.0) +
        COALESCE(kc1.exec_user_time, 0.0) + COALESCE(kc2.exec_user_time, 0.0) +
        COALESCE(kc1.exec_system_time, 0.0) + COALESCE(kc2.exec_system_time, 0.0) DESC,
        COALESCE(kc1.datid,kc2.datid),
        COALESCE(kc1.userid,kc2.userid),
        COALESCE(kc1.queryid,kc2.queryid),
        COALESCE(kc1.toplevel,kc2.toplevel)
        ) t1
    WHERE least(
        time1,
        time2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
        '<tr>'
          '<th rowspan="2">Query ID</th>'
          '<th rowspan="2">Database</th>'
          '<th rowspan="2">User</th>'
          '<th rowspan="2">I</th>'
          '<th title="Userspace CPU" colspan="{rusage_planstats?cputime_colspan}">User Time</th>'
          '<th title="Kernelspace CPU" colspan="{rusage_planstats?cputime_colspan}">System Time</th>'
        '</tr>'
        '<tr>'
          '{rusage_planstats?user_plan_time_hdr}'
          '<th title="User CPU time elapsed during execution">Exec (s)</th>'
          '<th title="User CPU time elapsed by this statement as a percentage of total user CPU time">%Total</th>'
          '{rusage_planstats?system_plan_time_hdr}'
          '<th title="System CPU time elapsed during execution">Exec (s)</th>'
          '<th title="System CPU time elapsed by this statement as a percentage of total system CPU time">%Total</th>'
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
          '{rusage_planstats?user_plan_time_row1}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{rusage_planstats?system_plan_time_row1}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '{rusage_planstats?user_plan_time_row2}'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '{rusage_planstats?system_plan_time_row2}'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'rusage_planstats?cputime_colspan','3',
      '!rusage_planstats?cputime_colspan','2',
      'rusage_planstats?user_plan_time_hdr',
        '<th title="User CPU time elapsed during planning">Plan (s)</th>',
      'rusage_planstats?system_plan_time_hdr',
        '<th title="System CPU time elapsed during planning">Plan (s)</th>',
      'rusage_planstats?user_plan_time_row1',
        '<td {value}>%6$s</td>',
      'rusage_planstats?system_plan_time_row1',
        '<td {value}>%9$s</td>',
      'rusage_planstats?user_plan_time_row2',
        '<td {value}>%12$s</td>',
      'rusage_planstats?system_plan_time_row2',
        '<td {value}>%15$s</td>'
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
            round(CAST(r_result.plan_user_time1 AS numeric),2),
            round(CAST(r_result.exec_user_time1 AS numeric),2),
            round(CAST(r_result.user_time_pct1 AS numeric),2),
            round(CAST(r_result.plan_system_time1 AS numeric),2),
            round(CAST(r_result.exec_system_time1 AS numeric),2),
            round(CAST(r_result.system_time_pct1 AS numeric),2),
            round(CAST(r_result.plan_user_time2 AS numeric),2),
            round(CAST(r_result.exec_user_time2 AS numeric),2),
            round(CAST(r_result.user_time_pct2 AS numeric),2),
            round(CAST(r_result.plan_system_time2 AS numeric),2),
            round(CAST(r_result.exec_system_time2 AS numeric),2),
            round(CAST(r_result.system_time_pct2 AS numeric),2)
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

CREATE FUNCTION top_io_filesystem_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR(topn integer) FOR
    SELECT
        kc.datid as datid,
        kc.dbname as dbname,
        kc.userid as userid,
        kc.username as username,
        kc.queryid as queryid,
        kc.toplevel as toplevel,
        NULLIF(kc.plan_reads, 0) as plan_reads,
        NULLIF(kc.exec_reads, 0) as exec_reads,
        NULLIF(kc.reads_total_pct, 0.0) as reads_total_pct,
        NULLIF(kc.plan_writes, 0)  as plan_writes,
        NULLIF(kc.exec_writes, 0)  as exec_writes,
        NULLIF(kc.writes_total_pct, 0.0) as writes_total_pct
    FROM top_kcache_statements1 kc
    WHERE COALESCE(kc.plan_reads, 0) + COALESCE(kc.plan_writes, 0) +
      COALESCE(kc.exec_reads, 0) + COALESCE(kc.exec_writes, 0) > 0
    ORDER BY COALESCE(kc.plan_reads, 0) + COALESCE(kc.plan_writes, 0) +
      COALESCE(kc.exec_reads, 0) + COALESCE(kc.exec_writes, 0) DESC,
      kc.datid,
      kc.userid,
      kc.queryid,
      kc.toplevel
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
            '<th title="Filesystem reads" colspan="{rusage_planstats?fs_colspan}">Read Bytes</th>'
            '<th title="Filesystem writes" colspan="{rusage_planstats?fs_colspan}">Write Bytes</th>'
          '</tr>'
          '<tr>'
            '{rusage_planstats?plan_reads_hdr}'
            '<th title="Filesystem read amount during execution">Exec</th>'
            '<th title="Filesystem read amount of this statement as a percentage of all statements FS read amount">%Total</th>'
            '{rusage_planstats?plan_writes_hdr}'
            '<th title="Filesystem write amount during execution">Exec</th>'
            '<th title="Filesystem write amount of this statement as a percentage of all statements FS write amount">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%5$s</td>'
          '{rusage_planstats?plan_reads_row}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{rusage_planstats?plan_writes_row}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'rusage_planstats?fs_colspan','3',
      '!rusage_planstats?fs_colspan','2',
      'rusage_planstats?plan_reads_hdr',
        '<th title="Filesystem read amount during planning">Plan</th>',
      'rusage_planstats?plan_writes_hdr',
        '<th title="Filesystem write amount during planning">Plan</th>',
      'rusage_planstats?plan_reads_row',
        '<td {value}>%6$s</td>',
      'rusage_planstats?plan_writes_row',
        '<td {value}>%9$s</td>'
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
            pg_size_pretty(r_result.plan_reads),
            pg_size_pretty(r_result.exec_reads),
            round(CAST(r_result.reads_total_pct AS numeric),2),
            pg_size_pretty(r_result.plan_writes),
            pg_size_pretty(r_result.exec_writes),
            round(CAST(r_result.writes_total_pct AS numeric),2)
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

CREATE FUNCTION top_io_filesystem_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(kc1.datid,kc2.datid) as datid,
        COALESCE(kc1.dbname,kc2.dbname) as dbname,
        COALESCE(kc1.userid,kc2.userid) as userid,
        COALESCE(kc1.username,kc2.username) as username,
        COALESCE(kc1.queryid,kc2.queryid) as queryid,
        COALESCE(kc1.toplevel,kc2.toplevel) as toplevel,
        NULLIF(kc1.plan_reads, 0) as plan_reads1,
        NULLIF(kc1.exec_reads, 0) as exec_reads1,
        NULLIF(kc1.reads_total_pct, 0.0) as reads_total_pct1,
        NULLIF(kc1.plan_writes, 0)  as plan_writes1,
        NULLIF(kc1.exec_writes, 0)  as exec_writes1,
        NULLIF(kc1.writes_total_pct, 0.0) as writes_total_pct1,
        NULLIF(kc2.plan_reads, 0) as plan_reads2,
        NULLIF(kc2.exec_reads, 0) as exec_reads2,
        NULLIF(kc2.reads_total_pct, 0.0) as reads_total_pct2,
        NULLIF(kc2.plan_writes, 0) as plan_writes2,
        NULLIF(kc2.exec_writes, 0) as exec_writes2,
        NULLIF(kc2.writes_total_pct, 0.0) as writes_total_pct2,
        row_number() OVER (ORDER BY COALESCE(kc1.exec_reads, 0.0) + COALESCE(kc1.exec_writes, 0.0) DESC NULLS LAST) as io_count1,
        row_number() OVER (ORDER BY COALESCE(kc2.exec_reads, 0.0) + COALESCE(kc2.exec_writes, 0.0)  DESC NULLS LAST) as io_count2
    FROM top_kcache_statements1 kc1
        FULL OUTER JOIN top_kcache_statements2 kc2 USING (server_id, datid, userid, queryid)
    WHERE COALESCE(kc1.plan_writes, 0.0) + COALESCE(kc2.plan_writes, 0.0) +
        COALESCE(kc1.plan_reads, 0.0) + COALESCE(kc2.plan_reads, 0.0) +
        COALESCE(kc1.exec_writes, 0.0) + COALESCE(kc2.exec_writes, 0.0) +
        COALESCE(kc1.exec_reads, 0.0) + COALESCE(kc2.exec_reads, 0.0) > 0
    ORDER BY COALESCE(kc1.plan_writes, 0.0) + COALESCE(kc2.plan_writes, 0.0) +
        COALESCE(kc1.plan_reads, 0.0) + COALESCE(kc2.plan_reads, 0.0) +
        COALESCE(kc1.exec_writes, 0.0) + COALESCE(kc2.exec_writes, 0.0) +
        COALESCE(kc1.exec_reads, 0.0) + COALESCE(kc2.exec_reads, 0.0) DESC,
        COALESCE(kc1.datid,kc2.datid),
        COALESCE(kc1.userid,kc2.userid),
        COALESCE(kc1.queryid,kc2.queryid),
        COALESCE(kc1.toplevel,kc2.toplevel)
        ) t1
    WHERE least(
        io_count1,
        io_count2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2">I</th>'
            '<th title="Filesystem reads" colspan="{rusage_planstats?fs_colspan}">Read Bytes</th>'
            '<th title="Filesystem writes" colspan="{rusage_planstats?fs_colspan}">Write Bytes</th>'
          '</tr>'
          '<tr>'
            '{rusage_planstats?plan_reads_hdr}'
            '<th title="Filesystem read amount during execution">Exec</th>'
            '<th title="Filesystem read amount of this statement as a percentage of all statements FS read amount">%Total</th>'
            '{rusage_planstats?plan_writes_hdr}'
            '<th title="Filesystem write amount during execution">Exec</th>'
            '<th title="Filesystem write amount of this statement as a percentage of all statements FS write amount">%Total</th>'
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
          '{rusage_planstats?plan_reads_row1}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{rusage_planstats?plan_writes_row1}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '{rusage_planstats?plan_reads_row2}'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '{rusage_planstats?plan_writes_row2}'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'rusage_planstats?fs_colspan','3',
      '!rusage_planstats?fs_colspan','2',
      'rusage_planstats?plan_reads_hdr',
        '<th title="Filesystem read amount during planning">Plan</th>',
      'rusage_planstats?plan_writes_hdr',
        '<th title="Filesystem write amount during planning">Plan</th>',
      'rusage_planstats?plan_reads_row1',
        '<td {value}>%6$s</td>',
      'rusage_planstats?plan_writes_row1',
        '<td {value}>%9$s</td>',
      'rusage_planstats?plan_reads_row2',
        '<td {value}>%12$s</td>',
      'rusage_planstats?plan_writes_row2',
        '<td {value}>%15$s</td>'
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
            pg_size_pretty(r_result.plan_reads1),
            pg_size_pretty(r_result.exec_reads1),
            round(CAST(r_result.reads_total_pct1 AS numeric),2),
            pg_size_pretty(r_result.plan_writes1),
            pg_size_pretty(r_result.exec_writes1),
            round(CAST(r_result.writes_total_pct1 AS numeric),2),
            pg_size_pretty(r_result.plan_reads2),
            pg_size_pretty(r_result.exec_reads2),
            round(CAST(r_result.reads_total_pct2 AS numeric),2),
            pg_size_pretty(r_result.plan_writes2),
            pg_size_pretty(r_result.exec_writes2),
            round(CAST(r_result.writes_total_pct2 AS numeric),2)
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

/*===== Settings reporting functions =====*/
CREATE FUNCTION settings_and_changes(IN sserver_id integer, IN start_id integer, IN end_id integer)
  RETURNS TABLE(
    first_seen          timestamp(0) with time zone,
    setting_scope       smallint,
    name                text,
    setting             text,
    reset_val           text,
    boot_val            text,
    unit                text,
    sourcefile          text,
    sourceline          integer,
    pending_restart     boolean,
    changed             boolean,
    default_val         boolean
  )
SET search_path=@extschema@ AS $$
  SELECT
    first_seen,
    setting_scope,
    name,
    setting,
    reset_val,
    boot_val,
    unit,
    sourcefile,
    sourceline,
    pending_restart,
    false,
    COALESCE(boot_val = reset_val, false)
  FROM v_sample_settings
  WHERE server_id = sserver_id AND sample_id = start_id
  UNION ALL
  SELECT
    first_seen,
    setting_scope,
    name,
    setting,
    reset_val,
    boot_val,
    unit,
    sourcefile,
    sourceline,
    pending_restart,
    true,
    COALESCE(boot_val = reset_val, false)
  FROM sample_settings s
    JOIN samples s_start ON (s_start.server_id = s.server_id AND s_start.sample_id = start_id)
    JOIN samples s_end ON (s_end.server_id = s.server_id AND s_end.sample_id = end_id)
  WHERE s.server_id = sserver_id AND s.first_seen > s_start.sample_time AND s.first_seen <= s_end.sample_time
$$ LANGUAGE SQL;

CREATE FUNCTION settings_and_changes_htbl(IN report_context jsonb, IN sserver_id integer)
  RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report_defined text := '';
    report_default text := '';
    defined_tpl    text := '';
    default_tpl    text := '';

    jtab_tpl       jsonb;
    notes          text[];

    --Cursor for top(cnt) queries ordered by elapsed time
    c_settings CURSOR(start1_id integer, end1_id integer) FOR
    SELECT
      first_seen,
      setting_scope,
      name,
      setting,
      reset_val,
      unit,
      sourcefile,
      sourceline,
      pending_restart,
      changed,
      default_val
    FROM settings_and_changes(sserver_id, start1_id, end1_id) st
    ORDER BY default_val AND NOT changed ASC, name,setting_scope,first_seen,pending_restart ASC NULLS FIRST;

    r_result RECORD;
BEGIN

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table class="setlist">'
          '{defined_tpl}'
          '{default_tpl}'
        '</table>',
      'defined_tpl',
        '<tr><th colspan="5">Defined settings</th></tr>'
        '<tr>'
          '<th>Setting</th>'
          '<th>reset_val</th>'
          '<th>Unit</th>'
          '<th>Source</th>'
          '<th>Notes</th>'
        '</tr>'
        '{rows_defined}',
      'default_tpl',
        '<tr><th colspan="5">Default settings</th></tr>'
        '<tr>'
           '<th>Setting</th>'
           '<th>reset_val</th>'
           '<th>Unit</th>'
           '<th>Source</th>'
           '<th>Notes</th>'
         '</tr>'
         '{rows_default}',
      'init_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'new_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}><strong>%s</strong></td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}><strong>%s</strong></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_settings(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer
      )
    LOOP
        notes := ARRAY[''];
        IF r_result.changed THEN
          notes := array_append(notes,r_result.first_seen::text);
        END IF;
        IF r_result.pending_restart THEN
          notes := array_append(notes,'Pending restart');
        END IF;
        notes := array_remove(notes,'');
        IF r_result.default_val AND NOT r_result.changed THEN
            report_default := report_default||format(
              jtab_tpl #>> ARRAY['init_tpl'],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,', ')
          );
        ELSIF NOT r_result.changed THEN
            report_defined := report_defined ||format(
              jtab_tpl #>> ARRAY['init_tpl'],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,',  ')
          );
        ELSE
            report_defined := report_defined ||format(
              jtab_tpl #>> ARRAY['new_tpl'],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,',  ')
          );
        END IF;
    END LOOP;

    IF report_default = '' and report_defined = '' THEN
        RETURN '!!!';
    ELSE
        -- apply settings to templates
        defined_tpl := replace(jtab_tpl #>> ARRAY['defined_tpl'],'{rows_defined}', report_defined);
        defined_tpl := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{defined_tpl}', defined_tpl);

        IF report_default != '' THEN
          default_tpl := replace(jtab_tpl #>> ARRAY['default_tpl'],'{rows_default}', report_default);
          RETURN replace(defined_tpl,'{default_tpl}',default_tpl);
        END IF;
        RETURN defined_tpl;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION settings_and_changes_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report_defined text := '';
    report_default text := '';
    defined_tpl    text := '';
    default_tpl    text := '';

    jtab_tpl    jsonb;
    notes       text[];

    v_init_tpl  text;
    v_new_tpl   text;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_settings CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer)
    FOR
    SELECT
      first_seen,
      setting_scope,
      st1.name as name1,
      st2.name as name2,
      name,
      setting,
      reset_val,
      COALESCE(st1.unit,st2.unit) as unit,
      COALESCE(st1.sourcefile,st2.sourcefile) as sourcefile,
      COALESCE(st1.sourceline,st2.sourceline) as sourceline,
      pending_restart,
      changed,
      default_val
    FROM settings_and_changes(sserver_id, start1_id, end1_id) st1
      FULL OUTER JOIN settings_and_changes(sserver_id, start2_id, end2_id) st2
        USING(first_seen, setting_scope, name, setting, reset_val, pending_restart, changed, default_val)
    ORDER BY default_val AND NOT changed ASC, name,setting_scope,first_seen,pending_restart ASC NULLS FIRST;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table class="setlist">'
          '{defined_tpl}'
          '{default_tpl}'
        '</table>',
      'defined_tpl',
        '<tr><th colspan="5">Defined settings</th></tr>'
          '<tr>'
            '<th>Setting</th>'
            '<th>reset_val</th>'
            '<th>Unit</th>'
            '<th>Source</th>'
            '<th>Notes</th>'
          '</tr>'
          '{rows_defined}',
      'default_tpl',
        '<tr><th colspan="5">Default settings</th></tr>'
          '<tr>'
            '<th>Setting</th>'
            '<th>reset_val</th>'
            '<th>Unit</th>'
            '<th>Source</th>'
            '<th>Notes</th>'
          '</tr>'
          '{rows_default}',
      'init_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'new_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}><strong>%s</strong></td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}><strong>%s</strong></td>'
        '</tr>',
      'init_tpl_i1',
        '<tr {interval1}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'new_tpl_i1',
        '<tr {interval1}>'
          '<td>%s</td>'
          '<td {value}><strong>%s</strong></td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}><strong>%s</strong></td>'
        '</tr>',
      'init_tpl_i2',
        '<tr {interval2}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'new_tpl_i2',
        '<tr {interval2}>'
          '<td>%s</td>'
          '<td {value}><strong>%s</strong></td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}><strong>%s</strong></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_settings(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
      CASE
        WHEN r_result.name1 IS NULL THEN
          v_init_tpl := 'init_tpl_i2';
          v_new_tpl := 'new_tpl_i2';
        WHEN r_result.name2 IS NULL THEN
          v_init_tpl := 'init_tpl_i1';
          v_new_tpl := 'new_tpl_i1';
        ELSE
          v_init_tpl := 'init_tpl';
          v_new_tpl := 'new_tpl';
      END CASE;
        notes := ARRAY[''];
        IF r_result.changed THEN
          notes := array_append(notes,r_result.first_seen::text);
        END IF;
        IF r_result.pending_restart THEN
          notes := array_append(notes,'Pending restart');
        END IF;
        notes := array_remove(notes,'');
        IF r_result.default_val AND NOT r_result.changed THEN
          report_default := report_default||format(
              jtab_tpl #>> ARRAY[v_init_tpl],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,',')
          );
        ELSIF NOT r_result.changed THEN
          report_defined := report_defined||format(
              jtab_tpl #>> ARRAY[v_init_tpl],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,',')
          );
        ELSE
          report_defined := report_defined||format(
              jtab_tpl #>> ARRAY[v_new_tpl],
              r_result.name,
              r_result.reset_val,
              r_result.unit,
              r_result.sourcefile || ':' || r_result.sourceline::text,
              array_to_string(notes,',')
          );
        END IF;

    END LOOP;

    IF report_default = '' and report_defined = '' THEN
        RETURN '!!!';
    ELSE
        -- apply settings to templates
        defined_tpl := replace(jtab_tpl #>> ARRAY['defined_tpl'],'{rows_defined}', report_defined);
        defined_tpl := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{defined_tpl}', defined_tpl);

        IF report_default != '' THEN
          default_tpl := replace(jtab_tpl #>> ARRAY['default_tpl'],'{rows_default}', report_default);
          RETURN replace(defined_tpl,'{default_tpl}',default_tpl);
        END IF;
        RETURN defined_tpl;
    END IF;
END;
$$ LANGUAGE plpgsql;
/* ===== pg_stat_statements checks ===== */

CREATE FUNCTION check_stmt_cnt(IN sserver_id integer, IN start_id integer = 0, IN end_id integer = 0) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    tab_tpl CONSTANT text :=
      '<table {stattbl}>'
        '<tr>'
          '<th>Sample ID</th>'
          '<th>Sample Time</th>'
          '<th>Stmts Captured</th>'
          '<th>pg_stat_statements.max</th>'
        '</tr>'
        '{rows}'
      '</table>';
    row_tpl CONSTANT text :=
      '<tr>'
        '<td>%s</td>'
        '<td>%s</td>'
        '<td>%s</td>'
        '<td>%s</td>'
      '</tr>';

    report text := '';

    c_stmt_all_stats CURSOR FOR
    SELECT sample_id,sample_time,stmt_cnt,prm.setting AS max_cnt
    FROM samples
        JOIN (
            SELECT sample_id,sum(statements) stmt_cnt
            FROM sample_statements_total
            WHERE server_id = sserver_id
            GROUP BY sample_id
        ) sample_stmt_cnt USING(sample_id)
        JOIN v_sample_settings prm USING (server_id, sample_id)
    WHERE server_id = sserver_id AND prm.name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer)
    ORDER BY sample_id ASC;

    c_stmt_stats CURSOR (s_id integer, e_id integer) FOR
    SELECT sample_id,sample_time,stmt_cnt,prm.setting AS max_cnt
    FROM samples
        JOIN (
            SELECT sample_id,sum(statements) stmt_cnt
            FROM sample_statements_total
            WHERE server_id = sserver_id AND sample_id BETWEEN s_id + 1 AND e_id
            GROUP BY sample_id
        ) sample_stmt_cnt USING(sample_id)
        JOIN v_sample_settings prm USING (server_id,sample_id)
    WHERE server_id = sserver_id AND prm.name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer)
    ORDER BY sample_id ASC;

    r_result RECORD;
BEGIN
    IF start_id = 0 THEN
        FOR r_result IN c_stmt_all_stats LOOP
            report := report||format(
                row_tpl,
                r_result.sample_id,
                r_result.sample_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    ELSE
        FOR r_result IN c_stmt_stats(start_id,end_id) LOOP
            report := report||format(
                row_tpl,
                r_result.sample_id,
                r_result.sample_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    END IF;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION check_stmt_cnt_first_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
SELECT check_stmt_cnt(
  sserver_id,
  (report_context #>> '{report_properties,start1_id}')::integer,
  (report_context #>> '{report_properties,end1_id}')::integer
)
$$ LANGUAGE sql;

CREATE FUNCTION check_stmt_cnt_second_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
SELECT check_stmt_cnt(
  sserver_id,
  (report_context #>> '{report_properties,start2_id}')::integer,
  (report_context #>> '{report_properties,end2_id}')::integer
)
$$ LANGUAGE sql;

CREATE FUNCTION check_stmt_cnt_all_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
SELECT check_stmt_cnt(
  sserver_id,
  0,
  0
)
$$ LANGUAGE sql;
/* ========= Check available statement stats for report ========= */

CREATE FUNCTION profile_checkavail_statstatements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there was available pg_stat_statements statistics for report interval
  SELECT count(sn.sample_id) = count(st.sample_id)
  FROM samples sn LEFT OUTER JOIN sample_statements_total st USING (server_id, sample_id)
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_planning_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(total_plan_time), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_stmt_wal_bytes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have statement wal sizes collected for report interval
  SELECT COALESCE(sum(wal_bytes), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_statements_jit_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
    SELECT COALESCE(sum(jit_functions + jit_inlining_count + jit_optimization_count + jit_emission_count), 0) > 0
    FROM sample_statements_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

/* ========= Statement stats functions ========= */

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

CREATE FUNCTION statements_stats_htbl(IN report_context jsonb, IN sserver_id integer)
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
        NULLIF(sum(blk_read_time), 0.0) as blk_read_time,
        NULLIF(sum(blk_write_time), 0.0) as blk_write_time,
        NULLIF(sum(trg_fn_total_time), 0.0) as trg_fn_total_time,
        NULLIF(sum(shared_gets), 0) as shared_gets,
        NULLIF(sum(local_gets), 0) as local_gets,
        NULLIF(sum(shared_blks_dirtied), 0) as shared_blks_dirtied,
        NULLIF(sum(local_blks_dirtied), 0) as local_blks_dirtied,
        NULLIF(sum(temp_blks_read), 0) as temp_blks_read,
        NULLIF(sum(temp_blks_written), 0) as temp_blks_written,
        NULLIF(sum(local_blks_read), 0) as local_blks_read,
        NULLIF(sum(local_blks_written), 0) as local_blks_written,
        NULLIF(sum(statements), 0) as statements,
        NULLIF(sum(wal_bytes), 0) as wal_bytes
    FROM statements_stats(sserver_id,start1_id,end1_id,topn)
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
            '<th colspan="2" title="Number of blocks fetched (hit + read)">Fetched (blk)</th>'
            '<th colspan="2" title="Number of blocks dirtied">Dirtied (blk)</th>'
            '<th colspan="2" title="Number of blocks, used in operations (like sorts and joins)">Temp (blk)</th>'
            '<th colspan="2" title="Number of blocks, used for temporary tables">Local (blk)</th>'
            '<th rowspan="2">Statements</th>'
            '{statement_wal_bytes?wal_bytes_hdr}'
          '</tr>'
          '<tr>'
            '{planning_times?plan_time_hdr}'
            '<th title="Time spent executing queries">Exec</th>'
            '<th title="Time spent reading blocks">Read</th>'   -- I/O time
            '<th title="Time spent writing blocks">Write</th>'
            '<th title="Time spent in trigger functions">Trg</th>'    -- Trigger functions time
            '<th>Shared</th>' -- Fetched
            '<th>Local</th>'
            '<th>Shared</th>' -- Dirtied
            '<th>Local</th>'
            '<th>Read</th>'   -- Work area read blks
            '<th>Write</th>'  -- Work area write blks
            '<th>Read</th>'   -- Local read blks
            '<th>Write</th>'  -- Local write blks
          '</tr>'
          '{rows}'
        '</table>',
      'stdb_tpl',
        '<tr>'
          '<td>%1$s</td>'
          '<td {value}>%2$s</td>'
          '{planning_times?plan_time_cell}'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '{statement_wal_bytes?wal_bytes_cell}'
        '</tr>',
      '!planning_times?time_hdr', -- Time header for stat_statements less then v1.8
        '<th colspan="4">Time (s)</th>',
      'planning_times?time_hdr', -- Time header for stat_statements v1.8 - added plan time field
        '<th colspan="5">Time (s)</th>',
      'planning_times?plan_time_hdr',
        '<th title="Time spent planning queries">Plan</th>',
      'planning_times?plan_time_cell',
        '<td {value}>%3$s</td>',
      'statement_wal_bytes?wal_bytes_hdr',
        '<th rowspan="2">WAL size</th>',
      'statement_wal_bytes?wal_bytes_cell',
        '<td {value}>%17$s</td>'
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
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2),
            round(CAST(r_result.trg_fn_total_time AS numeric),2),
            r_result.shared_gets,
            r_result.local_gets,
            r_result.shared_blks_dirtied,
            r_result.local_blks_dirtied,
            r_result.temp_blks_read,
            r_result.temp_blks_written,
            r_result.local_blks_read,
            r_result.local_blks_written,
            r_result.statements,
            pg_size_pretty(r_result.wal_bytes)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION statements_stats_diff_htbl(IN report_context jsonb, IN sserver_id integer)
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
        NULLIF(sum(st1.blk_read_time), 0.0) as blk_read_time1,
        NULLIF(sum(st1.blk_write_time), 0.0) as blk_write_time1,
        NULLIF(sum(st1.trg_fn_total_time), 0.0) as trg_fn_total_time1,
        NULLIF(sum(st1.shared_gets), 0) as shared_gets1,
        NULLIF(sum(st1.local_gets), 0) as local_gets1,
        NULLIF(sum(st1.shared_blks_dirtied), 0) as shared_blks_dirtied1,
        NULLIF(sum(st1.local_blks_dirtied), 0) as local_blks_dirtied1,
        NULLIF(sum(st1.temp_blks_read), 0) as temp_blks_read1,
        NULLIF(sum(st1.temp_blks_written), 0) as temp_blks_written1,
        NULLIF(sum(st1.local_blks_read), 0) as local_blks_read1,
        NULLIF(sum(st1.local_blks_written), 0) as local_blks_written1,
        NULLIF(sum(st1.statements), 0) as statements1,
        NULLIF(sum(st1.wal_bytes), 0) as wal_bytes1,
        NULLIF(sum(st2.calls), 0) as calls2,
        NULLIF(sum(st2.total_exec_time), 0.0) as total_exec_time2,
        NULLIF(sum(st2.total_plan_time), 0.0) as total_plan_time2,
        NULLIF(sum(st2.blk_read_time), 0.0) as blk_read_time2,
        NULLIF(sum(st2.blk_write_time), 0.0) as blk_write_time2,
        NULLIF(sum(st2.trg_fn_total_time), 0.0) as trg_fn_total_time2,
        NULLIF(sum(st2.shared_gets), 0) as shared_gets2,
        NULLIF(sum(st2.local_gets), 0) as local_gets2,
        NULLIF(sum(st2.shared_blks_dirtied), 0) as shared_blks_dirtied2,
        NULLIF(sum(st2.local_blks_dirtied), 0) as local_blks_dirtied2,
        NULLIF(sum(st2.temp_blks_read), 0) as temp_blks_read2,
        NULLIF(sum(st2.temp_blks_written), 0) as temp_blks_written2,
        NULLIF(sum(st2.local_blks_read), 0) as local_blks_read2,
        NULLIF(sum(st2.local_blks_written), 0) as local_blks_written2,
        NULLIF(sum(st2.statements), 0) as statements2,
        NULLIF(sum(st2.wal_bytes), 0) as wal_bytes2
    FROM statements_stats(sserver_id,start1_id,end1_id,topn) st1
        FULL OUTER JOIN statements_stats(sserver_id,start2_id,end2_id,topn) st2 USING (datid)
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
            '<th colspan="2" title="Number of blocks fetched (hit + read)">Fetched (blk)</th>'
            '<th colspan="2" title="Number of blocks dirtied">Dirtied (blk)</th>'
            '<th colspan="2" title="Number of blocks, used in operations (like sorts and joins)">Temp (blk)</th>'
            '<th colspan="2" title="Number of blocks, used for temporary tables">Local (blk)</th>'
            '<th rowspan="2">Statements</th>'
            '{statement_wal_bytes?wal_bytes_hdr}'
          '</tr>'
          '<tr>'
            '{planning_times?plan_time_hdr}'
            '<th title="Time spent executing queries">Exec</th>'
            '<th title="Time spent reading blocks">Read</th>'   -- I/O time
            '<th title="Time spent writing blocks">Write</th>'
            '<th title="Time spent in trigger functions">Trg</th>'    -- Trigger functions time
            '<th>Shared</th>' -- Fetched (blk)
            '<th>Local</th>'
            '<th>Shared</th>' -- Dirtied (blk)
            '<th>Local</th>'
            '<th>Read</th>'   -- Work area  blocks
            '<th>Write</th>'
            '<th>Read</th>'   -- Local blocks
            '<th>Write</th>'
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
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '{statement_wal_bytes?wal_bytes_cell1}'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%18$s</td>'
          '{planning_times?plan_time_cell2}'
          '<td {value}>%20$s</td>'
          '<td {value}>%21$s</td>'
          '<td {value}>%22$s</td>'
          '<td {value}>%23$s</td>'
          '<td {value}>%24$s</td>'
          '<td {value}>%25$s</td>'
          '<td {value}>%26$s</td>'
          '<td {value}>%27$s</td>'
          '<td {value}>%28$s</td>'
          '<td {value}>%29$s</td>'
          '<td {value}>%30$s</td>'
          '<td {value}>%31$s</td>'
          '<td {value}>%32$s</td>'
          '{statement_wal_bytes?wal_bytes_cell2}'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      '!planning_times?time_hdr', -- Time header for stat_statements less then v1.8
        '<th colspan="4">Time (s)</th>',
      'planning_times?time_hdr', -- Time header for stat_statements v1.8 - added plan time field
        '<th colspan="5">Time (s)</th>',
      'planning_times?plan_time_hdr',
        '<th title="Time spent planning queries">Plan</th>',
      'planning_times?plan_time_cell1',
        '<td {value}>%3$s</td>',
      'planning_times?plan_time_cell2',
        '<td {value}>%19$s</td>',
      'statement_wal_bytes?wal_bytes_hdr',
        '<th rowspan="2">WAL size</th>',
      'statement_wal_bytes?wal_bytes_cell1',
        '<td {value}>%17$s</td>',
      'statement_wal_bytes?wal_bytes_cell2',
        '<td {value}>%33$s</td>'
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
            round(CAST(r_result.blk_read_time1 AS numeric),2),
            round(CAST(r_result.blk_write_time1 AS numeric),2),
            round(CAST(r_result.trg_fn_total_time1 AS numeric),2),
            r_result.shared_gets1,
            r_result.local_gets1,
            r_result.shared_blks_dirtied1,
            r_result.local_blks_dirtied1,
            r_result.temp_blks_read1,
            r_result.temp_blks_written1,
            r_result.local_blks_read1,
            r_result.local_blks_written1,
            r_result.statements1,
            pg_size_pretty(r_result.wal_bytes1),
            r_result.calls2,
            round(CAST(r_result.total_plan_time2 AS numeric),2),
            round(CAST(r_result.total_exec_time2 AS numeric),2),
            round(CAST(r_result.blk_read_time2 AS numeric),2),
            round(CAST(r_result.blk_write_time2 AS numeric),2),
            round(CAST(r_result.trg_fn_total_time2 AS numeric),2),
            r_result.shared_gets2,
            r_result.local_gets2,
            r_result.shared_blks_dirtied2,
            r_result.local_blks_dirtied2,
            r_result.temp_blks_read2,
            r_result.temp_blks_written2,
            r_result.local_blks_read2,
            r_result.local_blks_written2,
            r_result.statements2,
            pg_size_pretty(r_result.wal_bytes2)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

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
/* ===== Statements stats functions ===== */

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

CREATE FUNCTION top_exec_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    -- Cursor for topn querues ordered by executions
    c_calls CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.calls, 0) as calls,
        NULLIF(st.calls_pct, 0.0) as calls_pct,
        NULLIF(st.total_exec_time, 0.0) as total_exec_time,
        NULLIF(st.min_exec_time, 0.0) as min_exec_time,
        NULLIF(st.max_exec_time, 0.0) as max_exec_time,
        NULLIF(st.mean_exec_time, 0.0) as mean_exec_time,
        NULLIF(st.stddev_exec_time, 0.0) as stddev_exec_time,
        NULLIF(st.rows, 0) as rows
    FROM top_statements1 st
    ORDER BY st.calls DESC,
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
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
            '<th title="Executions of this statement as a percentage of total executions of all statements in a cluster">%Total</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th>Mean(ms)</th>'
            '<th>Min(ms)</th>'
            '<th>Max(ms)</th>'
            '<th>StdErr(ms)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls(
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
            r_result.calls,
            round(CAST(r_result.calls_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_exec_time AS numeric),3),
            round(CAST(r_result.min_exec_time AS numeric),3),
            round(CAST(r_result.max_exec_time AS numeric),3),
            round(CAST(r_result.stddev_exec_time AS numeric),3),
            round(CAST(r_result.total_exec_time AS numeric),1)
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

CREATE FUNCTION top_exec_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    -- Cursor for topn querues ordered by executions
    c_calls CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st1.calls_pct, 0.0) as calls_pct1,
        NULLIF(st1.total_exec_time, 0.0) as total_exec_time1,
        NULLIF(st1.min_exec_time, 0.0) as min_exec_time1,
        NULLIF(st1.max_exec_time, 0.0) as max_exec_time1,
        NULLIF(st1.mean_exec_time, 0.0) as mean_exec_time1,
        NULLIF(st1.stddev_exec_time, 0.0) as stddev_exec_time1,
        NULLIF(st1.rows, 0) as rows1,
        NULLIF(st2.calls, 0) as calls2,
        NULLIF(st2.calls_pct, 0.0) as calls_pct2,
        NULLIF(st2.total_exec_time, 0.0) as total_exec_time2,
        NULLIF(st2.min_exec_time, 0.0) as min_exec_time2,
        NULLIF(st2.max_exec_time, 0.0) as max_exec_time2,
        NULLIF(st2.mean_exec_time, 0.0) as mean_exec_time2,
        NULLIF(st2.stddev_exec_time, 0.0) as stddev_exec_time2,
        NULLIF(st2.rows, 0) as rows2,
        row_number() over (ORDER BY st1.calls DESC NULLS LAST) as rn_calls1,
        row_number() over (ORDER BY st2.calls DESC NULLS LAST) as rn_calls2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    ORDER BY COALESCE(st1.calls,0) + COALESCE(st2.calls,0) DESC,
      COALESCE(st1.total_time,0) + COALESCE(st2.total_time,0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_calls1,
        rn_calls2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Executions sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th>I</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
            '<th title="Executions of this statement as a percentage of total executions of all statements in a cluster">%Total</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th>Mean(ms)</th>'
            '<th>Min(ms)</th>'
            '<th>Max(ms)</th>'
            '<th>StdErr(ms)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
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
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls(
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
            r_result.calls1,
            round(CAST(r_result.calls_pct1 AS numeric),2),
            r_result.rows1,
            round(CAST(r_result.mean_exec_time1 AS numeric),3),
            round(CAST(r_result.min_exec_time1 AS numeric),3),
            round(CAST(r_result.max_exec_time1 AS numeric),3),
            round(CAST(r_result.stddev_exec_time1 AS numeric),3),
            round(CAST(r_result.total_exec_time1 AS numeric),1),
            r_result.calls2,
            round(CAST(r_result.calls_pct2 AS numeric),2),
            r_result.rows2,
            round(CAST(r_result.mean_exec_time2 AS numeric),3),
            round(CAST(r_result.min_exec_time2 AS numeric),3),
            round(CAST(r_result.max_exec_time2 AS numeric),3),
            round(CAST(r_result.stddev_exec_time2 AS numeric),3),
            round(CAST(r_result.total_exec_time2 AS numeric),1)
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

CREATE FUNCTION top_iowait_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by I/O Wait time
    c_iowait_time CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.io_time, 0.0) as io_time,
        NULLIF(st.blk_read_time, 0.0) as blk_read_time,
        NULLIF(st.blk_write_time, 0.0) as blk_write_time,
        NULLIF(st.io_time_pct, 0.0) as io_time_pct,
        NULLIF(st.shared_blks_read, 0) as shared_blks_read,
        NULLIF(st.local_blks_read, 0) as local_blks_read,
        NULLIF(st.temp_blks_read, 0) as temp_blks_read,
        NULLIF(st.shared_blks_written, 0) as shared_blks_written,
        NULLIF(st.local_blks_written, 0) as local_blks_written,
        NULLIF(st.temp_blks_written, 0) as temp_blks_written,
        NULLIF(st.calls, 0) as calls
    FROM top_statements1 st
    WHERE st.io_time > 0
    ORDER BY st.io_time DESC,
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
            '<th rowspan="2" title="Time spent by the statement reading and writing blocks">IO(s)</th>'
            '<th rowspan="2" title="Time spent by the statement reading blocks">R(s)</th>'
            '<th rowspan="2" title="Time spent by the statement writing blocks">W(s)</th>'
            '<th rowspan="2" title="I/O time of this statement as a percentage of total I/O time for all statements in a cluster">%Total</th>'
            '<th colspan="3" title="Number of blocks read by the statement">Reads</th>'
            '<th colspan="3" title="Number of blocks written by the statement">Writes</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of shared blocks read by the statement">Shr</th>'
            '<th title="Number of local blocks read by the statement (usually used for temporary tables)">Loc</th>'
            '<th title="Number of temp blocks read by the statement (usually used for operations like sorts and joins)">Tmp</th>'
            '<th title="Number of shared blocks written by the statement">Shr</th>'
            '<th title="Number of local blocks written by the statement (usually used for temporary tables)">Loc</th>'
            '<th title="Number of temp blocks written by the statement (usually used for operations like sorts and joins)">Tmp</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time(
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
            round(CAST(r_result.io_time AS numeric),3),
            round(CAST(r_result.blk_read_time AS numeric),3),
            round(CAST(r_result.blk_write_time AS numeric),3),
            round(CAST(r_result.io_time_pct AS numeric),2),
            round(CAST(r_result.shared_blks_read AS numeric)),
            round(CAST(r_result.local_blks_read AS numeric)),
            round(CAST(r_result.temp_blks_read AS numeric)),
            round(CAST(r_result.shared_blks_written AS numeric)),
            round(CAST(r_result.local_blks_written AS numeric)),
            round(CAST(r_result.temp_blks_written AS numeric)),
            round(CAST(r_result.total_time AS numeric),1),
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

CREATE FUNCTION top_iowait_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by I/O Wait time
    c_iowait_time CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st1.total_time, 0.0) as total_time1,
        NULLIF(st1.io_time, 0.0) as io_time1,
        NULLIF(st1.blk_read_time, 0.0) as blk_read_time1,
        NULLIF(st1.blk_write_time, 0.0) as blk_write_time1,
        NULLIF(st1.io_time_pct, 0.0) as io_time_pct1,
        NULLIF(st1.shared_blks_read, 0) as shared_blks_read1,
        NULLIF(st1.local_blks_read, 0) as local_blks_read1,
        NULLIF(st1.temp_blks_read, 0) as temp_blks_read1,
        NULLIF(st1.shared_blks_written, 0) as shared_blks_written1,
        NULLIF(st1.local_blks_written, 0) as local_blks_written1,
        NULLIF(st1.temp_blks_written, 0) as temp_blks_written1,
        NULLIF(st2.calls, 0) as calls2,
        NULLIF(st2.total_time, 0.0) as total_time2,
        NULLIF(st2.io_time, 0.0) as io_time2,
        NULLIF(st2.blk_read_time, 0.0) as blk_read_time2,
        NULLIF(st2.blk_write_time, 0.0) as blk_write_time2,
        NULLIF(st2.io_time_pct, 0.0) as io_time_pct2,
        NULLIF(st2.shared_blks_read, 0) as shared_blks_read2,
        NULLIF(st2.local_blks_read, 0) as local_blks_read2,
        NULLIF(st2.temp_blks_read, 0) as temp_blks_read2,
        NULLIF(st2.shared_blks_written, 0) as shared_blks_written2,
        NULLIF(st2.local_blks_written, 0) as local_blks_written2,
        NULLIF(st2.temp_blks_written, 0) as temp_blks_written2,
        row_number() over (ORDER BY st1.io_time DESC NULLS LAST) as rn_iotime1,
        row_number() over (ORDER BY st2.io_time DESC NULLS LAST) as rn_iotime2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    WHERE COALESCE(st1.io_time, 0.0) + COALESCE(st2.io_time, 0.0) > 0
    ORDER BY COALESCE(st1.io_time, 0.0) + COALESCE(st2.io_time, 0.0) DESC,
      COALESCE(st1.total_time, 0.0) + COALESCE(st2.total_time, 0.0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_iotime1,
        rn_iotime2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- IOWait time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Time spent by the statement reading and writing blocks">IO(s)</th>'
            '<th rowspan="2" title="Time spent by the statement reading blocks">R(s)</th>'
            '<th rowspan="2" title="Time spent by the statement writing blocks">W(s)</th>'
            '<th rowspan="2" title="I/O time of this statement as a percentage of total I/O time for all statements in a cluster">%Total</th>'
            '<th colspan="3" title="Number of blocks read by the statement">Reads</th>'
            '<th colspan="3" title="Number of blocks written by the statement">Writes</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of shared blocks read by the statement">Shr</th>'
            '<th title="Number of local blocks read by the statement (usually used for temporary tables)">Loc</th>'
            '<th title="Number of temp blocks read by the statement (usually used for operations like sorts and joins)">Tmp</th>'
            '<th title="Number of shared blocks written by the statement">Shr</th>'
            '<th title="Number of local blocks written by the statement (usually used for temporary tables)">Loc</th>'
            '<th title="Number of temp blocks written by the statement (usually used for operations like sorts and joins)">Tmp</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
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
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time(
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
            round(CAST(r_result.io_time1 AS numeric),3),
            round(CAST(r_result.blk_read_time1 AS numeric),3),
            round(CAST(r_result.blk_write_time1 AS numeric),3),
            round(CAST(r_result.io_time_pct1 AS numeric),2),
            round(CAST(r_result.shared_blks_read1 AS numeric)),
            round(CAST(r_result.local_blks_read1 AS numeric)),
            round(CAST(r_result.temp_blks_read1 AS numeric)),
            round(CAST(r_result.shared_blks_written1 AS numeric)),
            round(CAST(r_result.local_blks_written1 AS numeric)),
            round(CAST(r_result.temp_blks_written1 AS numeric)),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.calls1,
            round(CAST(r_result.io_time2 AS numeric),3),
            round(CAST(r_result.blk_read_time2 AS numeric),3),
            round(CAST(r_result.blk_write_time2 AS numeric),3),
            round(CAST(r_result.io_time_pct2 AS numeric),2),
            round(CAST(r_result.shared_blks_read2 AS numeric)),
            round(CAST(r_result.local_blks_read2 AS numeric)),
            round(CAST(r_result.temp_blks_read2 AS numeric)),
            round(CAST(r_result.shared_blks_written2 AS numeric)),
            round(CAST(r_result.local_blks_written2 AS numeric)),
            round(CAST(r_result.temp_blks_written2 AS numeric)),
            round(CAST(r_result.total_time2 AS numeric),1),
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

CREATE FUNCTION top_shared_blks_fetched_htbl(IN report_context jsonb, IN sserver_id integer)
  RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by shared_blks_fetched
    c_shared_blks_fetched CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.shared_blks_fetched, 0) as shared_blks_fetched,
        NULLIF(st.shared_blks_fetched_pct, 0.0) as shared_blks_fetched_pct,
        NULLIF(st.shared_hit_pct, 0.0) as shared_hit_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements1 st
    WHERE shared_blks_fetched > 0
    ORDER BY st.shared_blks_fetched DESC,
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
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th title="Shared blocks fetched (read and hit) by the statement">blks fetched</th>'
            '<th title="Shared blocks fetched by this statement as a percentage of all shared blocks fetched in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by shared_blks_fetched
    FOR r_result IN c_shared_blks_fetched(
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
            r_result.shared_blks_fetched,
            round(CAST(r_result.shared_blks_fetched_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE FUNCTION top_shared_blks_fetched_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by shared_blks_fetched
    c_shared_blks_fetched CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.total_time, 0.0) as total_time1,
        NULLIF(st1.rows, 0) as rows1,
        NULLIF(st1.shared_blks_fetched, 0) as shared_blks_fetched1,
        NULLIF(st1.shared_blks_fetched_pct, 0.0) as shared_blks_fetched_pct1,
        NULLIF(st1.shared_hit_pct, 0.0) as shared_hit_pct1,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st2.total_time, 0.0) as total_time2,
        NULLIF(st2.rows, 0) as rows2,
        NULLIF(st2.shared_blks_fetched, 0) as shared_blks_fetched2,
        NULLIF(st2.shared_blks_fetched_pct, 0.0) as shared_blks_fetched_pct2,
        NULLIF(st2.shared_hit_pct, 0.0) as shared_hit_pct2,
        NULLIF(st2.calls, 0) as calls2,
        row_number() over (ORDER BY st1.shared_blks_fetched DESC NULLS LAST) as rn_shared_blks_fetched1,
        row_number() over (ORDER BY st2.shared_blks_fetched DESC NULLS LAST) as rn_shared_blks_fetched2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    WHERE COALESCE(st1.shared_blks_fetched, 0) + COALESCE(st2.shared_blks_fetched, 0) > 0
    ORDER BY COALESCE(st1.shared_blks_fetched, 0) + COALESCE(st2.shared_blks_fetched, 0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_shared_blks_fetched1,
        rn_shared_blks_fetched2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Fetched (blk) sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th>I</th>'
            '<th title="Shared blocks fetched (read and hit) by the statement">blks fetched</th>'
            '<th title="Shared blocks fetched by this statement as a percentage of all shared blocks fetched in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by shared_blks_fetched
    FOR r_result IN c_shared_blks_fetched(
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
            r_result.shared_blks_fetched1,
            round(CAST(r_result.shared_blks_fetched_pct1 AS numeric),2),
            round(CAST(r_result.shared_hit_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.shared_blks_fetched2,
            round(CAST(r_result.shared_blks_fetched_pct2 AS numeric),2),
            round(CAST(r_result.shared_hit_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE FUNCTION top_shared_reads_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by reads
    c_sh_reads CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.shared_blks_read, 0) as shared_blks_read,
        NULLIF(st.read_pct, 0.0) as read_pct,
        NULLIF(st.shared_hit_pct, 0.0) as shared_hit_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements1 st
    WHERE st.shared_blks_read > 0
    ORDER BY st.shared_blks_read DESC,
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
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th title="Total number of shared blocks read by the statement">Reads</th>'
            '<th title="Shared blocks read by this statement as a percentage of all shared blocks read in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by reads
    FOR r_result IN c_sh_reads(
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
            r_result.shared_blks_read,
            round(CAST(r_result.read_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE FUNCTION top_shared_reads_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by reads
    c_sh_reads CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.total_time, 0.0) as total_time1,
        NULLIF(st1.rows, 0) as rows1,
        NULLIF(st1.shared_blks_read, 0.0) as shared_blks_read1,
        NULLIF(st1.read_pct, 0.0) as read_pct1,
        NULLIF(st1.shared_hit_pct, 0.0) as shared_hit_pct1,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st2.total_time, 0.0) as total_time2,
        NULLIF(st2.rows, 0) as rows2,
        NULLIF(st2.shared_blks_read, 0) as shared_blks_read2,
        NULLIF(st2.read_pct, 0.0) as read_pct2,
        NULLIF(st2.shared_hit_pct, 0.0) as shared_hit_pct2,
        NULLIF(st2.calls, 0) as calls2,
        row_number() over (ORDER BY st1.shared_blks_read DESC NULLS LAST) as rn_reads1,
        row_number() over (ORDER BY st2.shared_blks_read DESC NULLS LAST) as rn_reads2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    WHERE COALESCE(st1.shared_blks_read, 0) + COALESCE(st2.shared_blks_read, 0) > 0
    ORDER BY COALESCE(st1.shared_blks_read, 0) + COALESCE(st2.shared_blks_read, 0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_reads1,
        rn_reads2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Reads sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th>I</th>'
            '<th title="Total number of shared blocks read by the statement">Reads</th>'
            '<th title="Shared blocks read by this statement as a percentage of all shared blocks read in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by reads
    FOR r_result IN c_sh_reads(
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
            r_result.shared_blks_read1,
            round(CAST(r_result.read_pct1 AS numeric),2),
            round(CAST(r_result.shared_hit_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.shared_blks_read2,
            round(CAST(r_result.read_pct2 AS numeric),2),
            round(CAST(r_result.shared_hit_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE FUNCTION top_shared_dirtied_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared dirtied
    c_sh_dirt CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.shared_blks_dirtied, 0) as shared_blks_dirtied,
        NULLIF(st.dirtied_pct, 0.0) as dirtied_pct,
        NULLIF(st.shared_hit_pct, 0.0) as shared_hit_pct,
        NULLIF(st.wal_bytes, 0) as wal_bytes,
        NULLIF(st.wal_bytes_pct, 0.0) as wal_bytes_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements1 st
    WHERE st.shared_blks_dirtied > 0
    ORDER BY st.shared_blks_dirtied DESC,
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
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Shared blocks dirtied by this statement as a percentage of all shared blocks dirtied in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '{statement_wal_bytes?wal_header}'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'statement_wal_bytes?wal_header',
        '<th title="Total amount of WAL bytes generated by the statement">WAL</th>'
        '<th title="WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster">%Total</th>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{statement_wal_bytes?wal_row}'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'statement_wal_bytes?wal_row',
        '<td {value}>%9$s</td>'
        '<td {value}>%10$s</td>'
    );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by shared dirtied
    FOR r_result IN c_sh_dirt(
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
            r_result.shared_blks_dirtied,
            round(CAST(r_result.dirtied_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            pg_size_pretty(r_result.wal_bytes),
            round(CAST(r_result.wal_bytes_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE FUNCTION top_shared_dirtied_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared dirtied
    c_sh_dirt CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.total_time, 0.0) as total_time1,
        NULLIF(st1.rows, 0) as rows1,
        NULLIF(st1.shared_blks_dirtied, 0) as shared_blks_dirtied1,
        NULLIF(st1.dirtied_pct, 0.0) as dirtied_pct1,
        NULLIF(st1.shared_hit_pct, 0.0) as shared_hit_pct1,
        NULLIF(st1.wal_bytes, 0) as wal_bytes1,
        NULLIF(st1.wal_bytes_pct, 0.0) as wal_bytes_pct1,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st2.total_time, 0.0) as total_time2,
        NULLIF(st2.rows, 0) as rows2,
        NULLIF(st2.shared_blks_dirtied, 0) as shared_blks_dirtied2,
        NULLIF(st2.dirtied_pct, 0.0) as dirtied_pct2,
        NULLIF(st2.shared_hit_pct, 0.0) as shared_hit_pct2,
        NULLIF(st2.wal_bytes, 0) as wal_bytes2,
        NULLIF(st2.wal_bytes_pct, 0.0) as wal_bytes_pct2,
        NULLIF(st2.calls, 0) as calls2,
        row_number() over (ORDER BY st1.shared_blks_dirtied DESC NULLS LAST) as rn_dirtied1,
        row_number() over (ORDER BY st2.shared_blks_dirtied DESC NULLS LAST) as rn_dirtied2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    WHERE COALESCE(st1.shared_blks_dirtied, 0) + COALESCE(st2.shared_blks_dirtied, 0) > 0
    ORDER BY COALESCE(st1.shared_blks_dirtied, 0) + COALESCE(st2.shared_blks_dirtied, 0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_dirtied1,
        rn_dirtied2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Dirtied sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th>I</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Shared blocks dirtied by this statement as a percentage of all shared blocks dirtied in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '{statement_wal_bytes?wal_hdr}'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'statement_wal_bytes?wal_hdr',
        '<th title="Total amount of WAL bytes generated by the statement">WAL</th>'
        '<th title="WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster">%Total</th>',
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
          '{statement_wal_bytes?wal_row1}'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '{statement_wal_bytes?wal_row2}'
          '<td {value}>%19$s</td>'
          '<td {value}>%20$s</td>'
          '<td {value}>%21$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>',
      'statement_wal_bytes?wal_row1',
        '<td {value}>%9$s</td>'
        '<td {value}>%10$s</td>',
      'statement_wal_bytes?wal_row2',
        '<td {value}>%17$s</td>'
        '<td {value}>%18$s</td>'
    );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by shared dirtied
    FOR r_result IN c_sh_dirt(
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
            r_result.shared_blks_dirtied1,
            round(CAST(r_result.dirtied_pct1 AS numeric),2),
            round(CAST(r_result.shared_hit_pct1 AS numeric),2),
            pg_size_pretty(r_result.wal_bytes1),
            round(CAST(r_result.wal_bytes_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.shared_blks_dirtied2,
            round(CAST(r_result.dirtied_pct2 AS numeric),2),
            round(CAST(r_result.shared_hit_pct2 AS numeric),2),
            pg_size_pretty(r_result.wal_bytes2),
            round(CAST(r_result.wal_bytes_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE FUNCTION top_shared_written_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared written
    c_sh_wr CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.shared_blks_written, 0) as shared_blks_written,
        NULLIF(st.tot_written_pct, 0.0) as tot_written_pct,
        NULLIF(st.backend_written_pct, 0.0) as backend_written_pct,
        NULLIF(st.shared_hit_pct, 0.0) as shared_hit_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements1 st
    WHERE st.shared_blks_written > 0
    ORDER BY st.shared_blks_written DESC,
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
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th title="Total number of shared blocks written by the statement">Written</th>'
            '<th title="Shared blocks written by this statement as a percentage of all shared blocks written in a cluster (sum of pg_stat_bgwriter fields buffers_checkpoint, buffers_clean and buffers_backend)">%Total</th>'
            '<th title="Shared blocks written by this statement as a percentage total buffers written directly by a backends (buffers_backend of pg_stat_bgwriter view)">%BackendW</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by shared written
    FOR r_result IN c_sh_wr(
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
            r_result.shared_blks_written,
            round(CAST(r_result.tot_written_pct AS numeric),2),
            round(CAST(r_result.backend_written_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE FUNCTION top_shared_written_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by shared written
    c_sh_wr CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.total_time, 0.0) as total_time1,
        NULLIF(st1.rows, 0) as rows1,
        NULLIF(st1.shared_blks_written, 0) as shared_blks_written1,
        NULLIF(st1.tot_written_pct, 0.0) as tot_written_pct1,
        NULLIF(st1.backend_written_pct, 0.0) as backend_written_pct1,
        NULLIF(st1.shared_hit_pct, 0.0) as shared_hit_pct1,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st2.total_time, 0.0) as total_time2,
        NULLIF(st2.rows, 0) as rows2,
        NULLIF(st2.shared_blks_written, 0) as shared_blks_written2,
        NULLIF(st2.tot_written_pct, 0.0) as tot_written_pct2,
        NULLIF(st2.backend_written_pct, 0.0) as backend_written_pct2,
        NULLIF(st2.shared_hit_pct, 0.0) as shared_hit_pct2,
        NULLIF(st2.calls, 0) as calls2,
        row_number() over (ORDER BY st1.shared_blks_written DESC NULLS LAST) as rn_written1,
        row_number() over (ORDER BY st2.shared_blks_written DESC NULLS LAST) as rn_written2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    WHERE COALESCE(st1.shared_blks_written, 0) + COALESCE(st2.shared_blks_written, 0) > 0
    ORDER BY COALESCE(st1.shared_blks_written, 0) + COALESCE(st2.shared_blks_written, 0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_written1,
        rn_written2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Shared written sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th>I</th>'
            '<th title="Total number of shared blocks written by the statement">Written</th>'
            '<th title="Shared blocks written by this statement as a percentage of all shared blocks written in a cluster (sum of pg_stat_bgwriter fields buffers_checkpoint, buffers_clean and buffers_backend)">%Total</th>'
            '<th title="Shared blocks written by this statement as a percentage total buffers written directly by a backends (buffers_backend field of pg_stat_bgwriter view)">%BackendW</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by shared written
    FOR r_result IN c_sh_wr(
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
            r_result.shared_blks_written1,
            round(CAST(r_result.tot_written_pct1 AS numeric),2),
            round(CAST(r_result.backend_written_pct1 AS numeric),2),
            round(CAST(r_result.shared_hit_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.shared_blks_written2,
            round(CAST(r_result.tot_written_pct2 AS numeric),2),
            round(CAST(r_result.backend_written_pct2 AS numeric),2),
            round(CAST(r_result.shared_hit_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE FUNCTION top_wal_size_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by WAL bytes
    c_wal_size CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.wal_bytes, 0) as wal_bytes,
        NULLIF(st.wal_bytes_pct, 0.0) as wal_bytes_pct,
        NULLIF(st.shared_blks_dirtied, 0) as shared_blks_dirtied,
        NULLIF(st.wal_fpi, 0) as wal_fpi,
        NULLIF(st.wal_records, 0) as wal_records
    FROM top_statements1 st
    WHERE st.wal_bytes > 0
    ORDER BY st.wal_bytes DESC,
      st.queryid ASC,
      st.toplevel ASC,
      st.datid ASC,
      st.userid ASC,
      st.dbname ASC,
      st.username ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when WAL stats is available
    IF NOT jsonb_extract_path_text(report_context, 'report_features', 'statement_wal_bytes')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th title="Total amount of WAL bytes generated by the statement">WAL</th>'
            '<th title="WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster">%Total</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Total number of WAL full page images generated by the statement">WAL FPI</th>'
            '<th title="Total number of WAL records generated by the statement">WAL records</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_wal_size(
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
            pg_size_pretty(r_result.wal_bytes),
            round(CAST(r_result.wal_bytes_pct AS numeric),2),
            r_result.shared_blks_dirtied,
            r_result.wal_fpi,
            r_result.wal_records
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

CREATE FUNCTION top_wal_size_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by WAL bytes
    c_wal_size CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.wal_bytes, 0) as wal_bytes1,
        NULLIF(st1.wal_bytes_pct, 0.0) as wal_bytes_pct1,
        NULLIF(st1.shared_blks_dirtied, 0) as shared_blks_dirtied1,
        NULLIF(st1.wal_fpi, 0) as wal_fpi1,
        NULLIF(st1.wal_records, 0) as wal_records1,
        NULLIF(st2.wal_bytes, 0) as wal_bytes2,
        NULLIF(st2.wal_bytes_pct, 0.0) as wal_bytes_pct2,
        NULLIF(st2.shared_blks_dirtied, 0) as shared_blks_dirtied2,
        NULLIF(st2.wal_fpi, 0) as wal_fpi2,
        NULLIF(st2.wal_records, 0) as wal_records2,
        row_number() over (ORDER BY st1.wal_bytes DESC NULLS LAST) as rn_wal1,
        row_number() over (ORDER BY st2.wal_bytes DESC NULLS LAST) as rn_wal2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    WHERE COALESCE(st1.wal_bytes, 0) + COALESCE(st2.wal_bytes, 0) > 0
    ORDER BY COALESCE(st1.wal_bytes, 0) + COALESCE(st2.wal_bytes, 0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_wal1,
        rn_wal2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when WAL stats is available
    IF NOT jsonb_extract_path_text(report_context, 'report_features', 'statement_wal_bytes')::boolean THEN
      RETURN '';
    END IF;

    -- WAL sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>User</th>'
            '<th>I</th>'
            '<th title="Total amount of WAL bytes generated by the statement">WAL</th>'
            '<th title="WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster">%Total</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Total number of WAL full page images generated by the statement">WAL FPI</th>'
            '<th title="Total number of WAL records generated by the statement">WAL records</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by shared_blks_fetched
    FOR r_result IN c_wal_size(
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
            pg_size_pretty(r_result.wal_bytes1),
            round(CAST(r_result.wal_bytes_pct1 AS numeric),2),
            r_result.shared_blks_dirtied1,
            r_result.wal_fpi1,
            r_result.wal_records1,
            pg_size_pretty(r_result.wal_bytes2),
            round(CAST(r_result.wal_bytes_pct2 AS numeric),2),
            r_result.shared_blks_dirtied2,
            r_result.wal_fpi2,
            r_result.wal_records2
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

CREATE FUNCTION top_temp_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by temp usage
    c_temp CURSOR(topn integer) FOR
    SELECT
        st.datid,
        st.dbname,
        st.userid,
        st.username,
        st.queryid,
        st.toplevel,
        NULLIF(st.total_time, 0.0) as total_time,
        NULLIF(st.rows, 0) as rows,
        NULLIF(st.local_blks_fetched, 0) as local_blks_fetched,
        NULLIF(st.local_hit_pct, 0.0) as local_hit_pct,
        NULLIF(st.temp_blks_written, 0) as temp_blks_written,
        NULLIF(st.temp_write_total_pct, 0.0) as temp_write_total_pct,
        NULLIF(st.temp_blks_read, 0) as temp_blks_read,
        NULLIF(st.temp_read_total_pct, 0.0) as temp_read_total_pct,
        NULLIF(st.local_blks_written, 0) as local_blks_written,
        NULLIF(st.local_write_total_pct, 0.0) as local_write_total_pct,
        NULLIF(st.local_blks_read, 0) as local_blks_read,
        NULLIF(st.local_read_total_pct, 0.0) as local_read_total_pct,
        NULLIF(st.calls, 0) as calls
    FROM top_statements1 st
    WHERE COALESCE(st.temp_blks_read, 0) + COALESCE(st.temp_blks_written, 0) +
        COALESCE(st.local_blks_read, 0) + COALESCE(st.local_blks_written, 0) > 0
    ORDER BY COALESCE(st.temp_blks_read, 0) + COALESCE(st.temp_blks_written, 0) +
        COALESCE(st.local_blks_read, 0) + COALESCE(st.local_blks_written, 0) DESC,
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
            '<th rowspan="2" title="Number of local blocks fetched (hit + read)">Local fetched</th>'
            '<th rowspan="2" title="Local blocks hit percentage">Hits(%)</th>'
            '<th colspan="4" title="Number of blocks, used for temporary tables">Local (blk)</th>'
            '<th colspan="4" title="Number of blocks, used in operations (like sorts and joins)">Temp (blk)</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of written local blocks">Write</th>'
            '<th title="Percentage of all local blocks written">%Total</th>'
            '<th title="Number of read local blocks">Read</th>'
            '<th title="Percentage of all local blocks read">%Total</th>'
            '<th title="Number of written temp blocks">Write</th>'
            '<th title="Percentage of all temp blocks written">%Total</th>'
            '<th title="Number of read temp blocks">Read</th>'
            '<th title="Percentage of all temp blocks read">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td>%4$s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp(
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
            r_result.local_blks_fetched,
            round(CAST(r_result.local_hit_pct AS numeric),2),
            r_result.local_blks_written,
            round(CAST(r_result.local_write_total_pct AS numeric),2),
            r_result.local_blks_read,
            round(CAST(r_result.local_read_total_pct AS numeric),2),
            r_result.temp_blks_written,
            round(CAST(r_result.temp_write_total_pct AS numeric),2),
            r_result.temp_blks_read,
            round(CAST(r_result.temp_read_total_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE FUNCTION top_temp_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report      text := '';
    tab_row     text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by temp usage
    c_temp CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.datid,st2.datid) as datid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.userid,st2.userid) as userid,
        COALESCE(st1.username,st2.username) as username,
        COALESCE(st1.queryid,st2.queryid) as queryid,
        COALESCE(st1.toplevel,st2.toplevel) as toplevel,
        NULLIF(st1.total_time, 0.0) as total_time1,
        NULLIF(st1.rows, 0) as rows1,
        NULLIF(st1.local_blks_fetched, 0) as local_blks_fetched1,
        NULLIF(st1.local_hit_pct, 0.0) as local_hit_pct1,
        NULLIF(st1.temp_blks_written, 0) as temp_blks_written1,
        NULLIF(st1.temp_write_total_pct, 0.0) as temp_write_total_pct1,
        NULLIF(st1.temp_blks_read, 0) as temp_blks_read1,
        NULLIF(st1.temp_read_total_pct, 0.0) as temp_read_total_pct1,
        NULLIF(st1.local_blks_written, 0) as local_blks_written1,
        NULLIF(st1.local_write_total_pct, 0.0) as local_write_total_pct1,
        NULLIF(st1.local_blks_read, 0) as local_blks_read1,
        NULLIF(st1.local_read_total_pct, 0.0) as local_read_total_pct1,
        NULLIF(st1.calls, 0) as calls1,
        NULLIF(st2.total_time, 0.0) as total_time2,
        NULLIF(st2.rows, 0) as rows2,
        NULLIF(st2.local_blks_fetched, 0) as local_blks_fetched2,
        NULLIF(st2.local_hit_pct, 0.0) as local_hit_pct2,
        NULLIF(st2.temp_blks_written, 0) as temp_blks_written2,
        NULLIF(st2.temp_write_total_pct, 0.0) as temp_write_total_pct2,
        NULLIF(st2.temp_blks_read, 0) as temp_blks_read2,
        NULLIF(st2.temp_read_total_pct, 0.0) as temp_read_total_pct2,
        NULLIF(st2.local_blks_written, 0) as local_blks_written2,
        NULLIF(st2.local_write_total_pct, 0.0) as local_write_total_pct2,
        NULLIF(st2.local_blks_read, 0) as local_blks_read2,
        NULLIF(st2.local_read_total_pct, 0.0) as local_read_total_pct2,
        NULLIF(st2.calls, 0) as calls2,
        row_number() over (ORDER BY COALESCE(st1.temp_blks_read, 0)+ COALESCE(st1.temp_blks_written, 0)+
          COALESCE(st1.local_blks_read, 0)+ COALESCE(st1.local_blks_written, 0)DESC NULLS LAST) as rn_temp1,
        row_number() over (ORDER BY COALESCE(st2.temp_blks_read, 0)+ COALESCE(st2.temp_blks_written, 0)+
          COALESCE(st2.local_blks_read, 0)+ COALESCE(st2.local_blks_written, 0)DESC NULLS LAST) as rn_temp2
    FROM top_statements1 st1
        FULL OUTER JOIN top_statements2 st2 USING (server_id, datid, userid, queryid, toplevel)
    WHERE COALESCE(st1.temp_blks_read, 0) + COALESCE(st1.temp_blks_written, 0) +
        COALESCE(st1.local_blks_read, 0) + COALESCE(st1.local_blks_written, 0) +
        COALESCE(st2.temp_blks_read, 0) + COALESCE(st2.temp_blks_written, 0) +
        COALESCE(st2.local_blks_read, 0) + COALESCE(st2.local_blks_written, 0) > 0
    ORDER BY COALESCE(st1.temp_blks_read, 0) + COALESCE(st1.temp_blks_written, 0) +
        COALESCE(st1.local_blks_read, 0) + COALESCE(st1.local_blks_written, 0) +
        COALESCE(st2.temp_blks_read, 0) + COALESCE(st2.temp_blks_written, 0) +
        COALESCE(st2.local_blks_read, 0) + COALESCE(st2.local_blks_written, 0) DESC,
      COALESCE(st1.queryid,st2.queryid) ASC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.userid,st2.userid) ASC,
      COALESCE(st1.toplevel,st2.toplevel) ASC
    ) t1
    WHERE least(
        rn_temp1,
        rn_temp2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Temp usage sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">User</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of local blocks fetched (hit + read)">Local fetched</th>'
            '<th rowspan="2" title="Local blocks hit percentage">Hits(%)</th>'
            '<th colspan="4" title="Number of blocks, used for temporary tables">Local (blk)</th>'
            '<th colspan="4" title="Number of blocks, used in operations (like sorts and joins)">Temp (blk)</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of written local blocks">Write</th>'
            '<th title="Percentage of all local blocks written">%Total</th>'
            '<th title="Number of read local blocks">Read</th>'
            '<th title="Percentage of all local blocks read">%Total</th>'
            '<th title="Number of written temp blocks">Write</th>'
            '<th title="Percentage of all temp blocks written">%Total</th>'
            '<th title="Number of read temp blocks">Read</th>'
            '<th title="Percentage of all temp blocks read">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><p><a HREF="#%2$s">%2$s</a></p>'
          '<p><small>[%3$s]</small>%1$s</p></td>'
          '<td {rowtdspanhdr}>%4$s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
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
      'nested_tpl',
        ' <small title="Nested level">(N)</small>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp(
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
            r_result.local_blks_fetched1,
            round(CAST(r_result.local_hit_pct1 AS numeric),2),
            r_result.local_blks_written1,
            round(CAST(r_result.local_write_total_pct1 AS numeric),2),
            r_result.local_blks_read1,
            round(CAST(r_result.local_read_total_pct1 AS numeric),2),
            r_result.temp_blks_written1,
            round(CAST(r_result.temp_write_total_pct1 AS numeric),2),
            r_result.temp_blks_read1,
            round(CAST(r_result.temp_read_total_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.local_blks_fetched2,
            round(CAST(r_result.local_hit_pct2 AS numeric),2),
            r_result.local_blks_written2,
            round(CAST(r_result.local_write_total_pct2 AS numeric),2),
            r_result.local_blks_read2,
            round(CAST(r_result.local_read_total_pct2 AS numeric),2),
            r_result.temp_blks_written2,
            round(CAST(r_result.temp_write_total_pct2 AS numeric),2),
            r_result.temp_blks_read2,
            round(CAST(r_result.temp_read_total_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE FUNCTION collect_queries(IN userid oid, IN datid oid, queryid bigint)
RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    INSERT INTO queries_list(
      userid,
      datid,
      queryid
    )
    VALUES (
      collect_queries.userid,
      collect_queries.datid,
      collect_queries.queryid
    )
    ON CONFLICT DO NOTHING;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION report_queries(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    c_queries CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer)
    FOR
    SELECT
      queryid,
      ord,
      row_span,
      query
    FROM (
      SELECT
      queryid,
      row_number() OVER (PARTITION BY queryid
        ORDER BY
          last_sample_id DESC NULLS FIRST,
          queryid_md5 DESC NULLS FIRST
        ) ord,
      -- Calculate a value for statement rowspan atribute
      least(count(*) OVER (PARTITION BY queryid),3) row_span,
      query
      FROM (
        SELECT DISTINCT
          server_id,
          queryid,
          queryid_md5
        FROM
          queries_list ql
          JOIN sample_statements ss USING (datid, userid, queryid)
        WHERE
          ss.server_id = sserver_id
          AND (
            sample_id BETWEEN start1_id AND end1_id
            OR sample_id BETWEEN start2_id AND end2_id
          )
      ) queryids
      JOIN stmt_list USING (server_id, queryid_md5)
    ) ord_stmt_v
    WHERE ord <= 3
    ORDER BY
      queryid ASC,
      ord ASC;

    qr_result   RECORD;
    report      text := '';
    query_text  text := '';
    qlen_limit  integer;
    jtab_tpl    jsonb;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table class="stmtlist">'
          '<tr>'
            '<th>QueryID</th>'
            '<th>Query Text</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td class="mono hdr" id="%1$s" rowspan="%3$s">%1$s</td>'
          '<td {mono}>%2$s</td>'
        '</tr>',
      'substmt_tpl',
        '<tr>'
          '<td {mono}>%1$s</td>'
        '</tr>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    qlen_limit := (report_context #>> '{report_properties,max_query_length}')::integer;

    FOR qr_result IN c_queries(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        query_text := replace(qr_result.query,'<','&lt;');
        query_text := replace(query_text,'>','&gt;');
        IF qr_result.ord = 1 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['stmt_tpl'],
              to_hex(qr_result.queryid),
              left(query_text,qlen_limit),
              qr_result.row_span
          );
        ELSE
          report := report||format(
              jtab_tpl #>> ARRAY['substmt_tpl'],
              left(query_text,qlen_limit)
          );
        END IF;
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
/* pg_wait_sampling reporting functions */
CREATE FUNCTION profile_checkavail_wait_sampling_total(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there is table sizes collected in both bounds
  SELECT
    count(*) > 0
  FROM wait_sampling_total
  WHERE
    server_id = sserver_id
    AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION wait_sampling_total_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        event_type text,
        event      text,
        tot_waited      numeric,
        stmt_waited     numeric
)
SET search_path=@extschema@ AS $$
    SELECT
        st.event_type,
        st.event,
        sum(st.tot_waited)::numeric / 1000 AS tot_waited,
        sum(st.stmt_waited)::numeric / 1000 AS stmt_waited
    FROM wait_sampling_total st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.event_type, st.event;
$$ LANGUAGE sql;

CREATE FUNCTION wait_sampling_totals_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for wait stats
    c_stats CURSOR
    FOR
    WITH tot AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats1)
    SELECT
        event_type,
        sum(st.tot_waited) as tot_waited,
        sum(st.tot_waited) * 100 / NULLIF(min(tot.tot_waited),0) as tot_waited_pct,
        sum(st.stmt_waited) as stmt_waited,
        sum(st.stmt_waited) * 100 / NULLIF(min(tot.stmt_waited),0) as stmt_waited_pct
    FROM wait_sampling_total_stats1 st CROSS JOIN tot
    GROUP BY ROLLUP(event_type)
    ORDER BY event_type NULLS LAST;

    r_result RECORD;
BEGIN
    -- wait stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th title="Time, waited in events of wait event type executing statements in seconds">Statements Waited (s)</th>'
            '<th title="Time, waited in events of wait event type as a percentage of total time waited in a cluster executing statements">%Total</th>'
            '<th title="Time, waited in events of wait event type by all backends (including background activity) in seconds">All Waited (s)</th>'
            '<th title="Time, waited in events of wait event type as a percentage of total time waited in a cluster by all backends (including background activity)">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wait_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'wait_tot_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td></td>'
          '<td {value}>%s</td>'
          '<td></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary wait stats
    FOR r_result IN c_stats
    LOOP
      IF r_result.event_type IS NOT NULL THEN
        report := report||format(
            jtab_tpl #>> ARRAY['wait_tpl'],
            r_result.event_type,
            round(r_result.stmt_waited, 2),
            round(r_result.stmt_waited_pct,2),
            round(r_result.tot_waited, 2),
            round(r_result.tot_waited_pct,2)
        );
      ELSE
        IF COALESCE(r_result.tot_waited,r_result.stmt_waited) IS NOT NULL THEN
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tot_tpl'],
              'Total',
              round(r_result.stmt_waited, 2),
              round(r_result.tot_waited, 2)
          );
        END IF;
      END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION wait_sampling_totals_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for wait stats
    c_stats CURSOR
    FOR
    WITH tot1 AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats1),
    tot2 AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats2)
    SELECT
        event_type,
        sum(st1.tot_waited) as tot_waited1,
        sum(st1.tot_waited) * 100 / NULLIF(min(tot1.tot_waited),0) as tot_waited_pct1,
        sum(st1.stmt_waited) as stmt_waited1,
        sum(st1.stmt_waited) * 100 / NULLIF(min(tot1.stmt_waited),0) as stmt_waited_pct1,
        sum(st2.tot_waited) as tot_waited2,
        sum(st2.tot_waited) * 100 / NULLIF(min(tot2.tot_waited),0) as tot_waited_pct2,
        sum(st2.stmt_waited) as stmt_waited2,
        sum(st2.stmt_waited) * 100 / NULLIF(min(tot2.stmt_waited),0) as stmt_waited_pct2
    FROM (wait_sampling_total_stats1 st1 CROSS JOIN tot1)
      FULL JOIN
        (wait_sampling_total_stats2 st2 CROSS JOIN tot2)
      USING (event_type, event)
    GROUP BY ROLLUP(event_type)
    ORDER BY event_type NULLS LAST;

    r_result RECORD;
BEGIN
    -- wait stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>I</th>'
            '<th title="Time, waited in events of wait event type executing statements in seconds">Statements Waited (s)</th>'
            '<th title="Time, waited in events of wait event type as a percentage of total time waited in a cluster executing statements">%Total</th>'
            '<th title="Time, waited in events of wait event type by all backends (including background activity) in seconds">All Waited (s)</th>'
            '<th title="Time, waited in events of wait event type as a percentage of total time waited in a cluster by all backends (including background activity)">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wait_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'wait_tot_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td></td>'
          '<td {value}>%s</td>'
          '<td></td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td></td>'
          '<td {value}>%s</td>'
          '<td></td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>'
        );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary wait stats
    FOR r_result IN c_stats
    LOOP
      IF r_result.event_type IS NOT NULL THEN
        report := report||format(
            jtab_tpl #>> ARRAY['wait_tpl'],
            r_result.event_type,
            round(r_result.stmt_waited1, 2),
            round(r_result.stmt_waited_pct1,2),
            round(r_result.tot_waited1, 2),
            round(r_result.tot_waited_pct1,2),
            round(r_result.stmt_waited2, 2),
            round(r_result.stmt_waited_pct2,2),
            round(r_result.tot_waited2, 2),
            round(r_result.tot_waited_pct2,2)
        );
      ELSE
        IF COALESCE(r_result.tot_waited1,r_result.stmt_waited1,r_result.tot_waited2,r_result.stmt_waited2) IS NOT NULL
        THEN
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tot_tpl'],
              'Total',
              round(r_result.stmt_waited1, 2),
              round(r_result.tot_waited1, 2),
              round(r_result.stmt_waited2, 2),
              round(r_result.tot_waited2, 2)
          );
        END IF;
      END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_wait_sampling_events_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for wait stats
    c_all_stats CURSOR(topn integer)
    FOR
    WITH tot AS (
      SELECT sum(tot_waited) AS tot_waited
      FROM wait_sampling_total_stats1)
    SELECT
        event_type,
        event,
        st.tot_waited,
        st.tot_waited * 100 / NULLIF(tot.tot_waited,0) as tot_waited_pct
    FROM wait_sampling_total_stats1 st CROSS JOIN tot
    WHERE st.tot_waited IS NOT NULL AND st.tot_waited > 0
    ORDER BY st.tot_waited DESC, st.event_type, st.event
    LIMIT topn;

    c_stmt_stats CURSOR(topn integer)
    FOR
    WITH tot AS (
      SELECT sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats1)
    SELECT
        event_type,
        event,
        st.stmt_waited,
        st.stmt_waited * 100 / NULLIF(tot.stmt_waited,0) as stmt_waited_pct
    FROM wait_sampling_total_stats1 st CROSS JOIN tot
    WHERE st.stmt_waited IS NOT NULL AND st.stmt_waited > 0
    ORDER BY st.stmt_waited DESC, st.event_type, st.event
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- wait stats TPLs
    jtab_tpl := jsonb_build_object(
      'wt_smp_all_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>Wait event</th>'
            '<th title="Time, waited in event by all backends (including background activity) in seconds">Waited (s)</th>'
            '<th title="Time, waited in event by all backends as a percentage of total time waited in a cluster by all backends (including background activity)">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wt_smp_stmt_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>Wait event</th>'
            '<th title="Time, waited in event executing statements in seconds">Waited (s)</th>'
            '<th title="Time, waited in event as a percentage of total time waited in a cluster executing statements">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wait_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting wait event stats
    CASE report_context #>> '{report_properties,sect_href}'
      WHEN 'wt_smp_all' THEN
        FOR r_result IN c_all_stats(
          (report_context #>> '{report_properties,topn}')::integer
        )
        LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tpl'],
              r_result.event_type,
              r_result.event,
              round(r_result.tot_waited, 2),
              round(r_result.tot_waited_pct,2)
          );
        END LOOP;
      WHEN 'wt_smp_stmt' THEN
        FOR r_result IN c_stmt_stats(
          (report_context #>> '{report_properties,topn}')::integer
        )
        LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tpl'],
              r_result.event_type,
              r_result.event,
              round(r_result.stmt_waited, 2),
              round(r_result.stmt_waited_pct,2)
          );
        END LOOP;
      ELSE
        RAISE 'Incorrect report context';
    END CASE;

    IF report != '' THEN
        report := replace(
          jtab_tpl #>> ARRAY[concat(report_context #>> '{report_properties,sect_href}','_hdr')],
          '{rows}',
          report
        );
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_wait_sampling_events_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for wait stats
    c_all_stats CURSOR(topn integer)
    FOR
    WITH tot1 AS (
      SELECT sum(tot_waited) AS tot_waited
      FROM wait_sampling_total_stats1),
    tot2 AS (
      SELECT sum(tot_waited) AS tot_waited
      FROM wait_sampling_total_stats2)
    SELECT
        event_type,
        event,
        st1.tot_waited as tot_waited1,
        st1.tot_waited * 100 / NULLIF(tot1.tot_waited,0) as tot_waited_pct1,
        st2.tot_waited as tot_waited2,
        st2.tot_waited * 100 / NULLIF(tot2.tot_waited,0) as tot_waited_pct2
    FROM (wait_sampling_total_stats1 st1 CROSS JOIN tot1)
      FULL JOIN
    (wait_sampling_total_stats2 st2 CROSS JOIN tot2)
      USING (event_type, event)
    WHERE num_nulls(st1.tot_waited,st2.tot_waited) < 2 AND
      COALESCE(st1.tot_waited,0) + COALESCE(st2.tot_waited,0) > 0
    ORDER BY COALESCE(st1.tot_waited,0) + COALESCE(st2.tot_waited,0) DESC, event_type, event
    LIMIT topn;

    c_stmt_stats CURSOR(topn integer)
    FOR
    WITH tot1 AS (
      SELECT sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats1),
    tot2 AS (
      SELECT sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats2)
    SELECT
        event_type,
        event,
        st1.stmt_waited as stmt_waited1,
        st1.stmt_waited * 100 / NULLIF(tot1.stmt_waited,0) as stmt_waited_pct1,
        st2.stmt_waited as stmt_waited2,
        st2.stmt_waited * 100 / NULLIF(tot2.stmt_waited,0) as stmt_waited_pct2
    FROM (wait_sampling_total_stats1 st1 CROSS JOIN tot1)
      FULL JOIN
    (wait_sampling_total_stats2 st2 CROSS JOIN tot2)
      USING (event_type, event)
    WHERE num_nulls(st1.stmt_waited,st2.stmt_waited) < 2 AND
      COALESCE(st1.stmt_waited,0) + COALESCE(st2.stmt_waited,0) > 0
    ORDER BY COALESCE(st1.stmt_waited,0) + COALESCE(st2.stmt_waited,0) DESC, event_type, event
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- wait stats TPLs
    jtab_tpl := jsonb_build_object(
      'wt_smp_all_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>Wait event</th>'
            '<th>I</th>'
            '<th title="Time, waited in event by all backends (including background activity) in seconds">Waited (s)</th>'
            '<th title="Time, waited in event by all backends as a percentage of total time waited in a cluster by all backends (including background activity)">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wt_smp_stmt_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>Wait event</th>'
            '<th>I</th>'
            '<th title="Time, waited in event executing statements in seconds">Waited (s)</th>'
            '<th title="Time, waited in event as a percentage of total time waited in a cluster executing statements">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wait_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>'
    );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting wait event stats
    CASE report_context #>> '{report_properties,sect_href}'
      WHEN 'wt_smp_all' THEN
        FOR r_result IN c_all_stats(
          (report_context #>> '{report_properties,topn}')::integer
        )
        LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tpl'],
              r_result.event_type,
              r_result.event,
              round(r_result.tot_waited1, 2),
              round(r_result.tot_waited_pct1,2),
              round(r_result.tot_waited2, 2),
              round(r_result.tot_waited_pct2,2)
          );
        END LOOP;
      WHEN 'wt_smp_stmt' THEN
        FOR r_result IN c_stmt_stats(
          (report_context #>> '{report_properties,topn}')::integer
        )
        LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tpl'],
              r_result.event_type,
              r_result.event,
              round(r_result.stmt_waited1, 2),
              round(r_result.stmt_waited_pct1,2),
              round(r_result.stmt_waited2, 2),
              round(r_result.stmt_waited_pct2,2)
          );
        END LOOP;
      ELSE
        RAISE 'Incorrect report context';
    END CASE;

    IF report != '' THEN
        report := replace(
          jtab_tpl #>> ARRAY[concat(report_context #>> '{report_properties,sect_href}','_hdr')],
          '{rows}',
          report
        );
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;
/* ===== Tables stats functions ===== */

CREATE FUNCTION tablespace_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id integer,
    tablespaceid oid,
    tablespacename name,
    tablespacepath text,
    size_delta bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.tablespaceid,
        st.tablespacename,
        st.tablespacepath,
        sum(st.size_delta)::bigint AS size_delta
    FROM v_sample_stat_tablespaces st
    WHERE st.server_id = sserver_id
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.tablespaceid, st.tablespacename, st.tablespacepath
$$ LANGUAGE sql;

CREATE FUNCTION tablespaces_stats_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR(start1_id integer, end1_id integer) FOR
    SELECT
        st.tablespacename,
        st.tablespacepath,
        pg_size_pretty(NULLIF(st_last.size, 0)) as size,
        pg_size_pretty(NULLIF(st.size_delta, 0)) as size_delta
    FROM tablespace_stats(sserver_id,start1_id,end1_id) st
      LEFT OUTER JOIN v_sample_stat_tablespaces st_last ON
        (st_last.server_id = st.server_id AND st_last.sample_id = end1_id AND st_last.tablespaceid = st.tablespaceid)
    ORDER BY st.tablespacename ASC;

    r_result RECORD;
BEGIN
       --- Populate templates

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Tablespace</th>'
            '<th>Path</th>'
            '<th title="Tablespace size as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Tablespace size increment during report interval">Growth</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer
      )
    LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['ts_tpl'],
              r_result.tablespacename,
              r_result.tablespacepath,
              r_result.size,
              r_result.size_delta
          );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;


    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tablespaces_stats_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer)
    FOR
    SELECT
        COALESCE(stat1.tablespacename,stat2.tablespacename) AS tablespacename,
        COALESCE(stat1.tablespacepath,stat2.tablespacepath) AS tablespacepath,
        pg_size_pretty(NULLIF(st_last1.size, 0)) as size1,
        pg_size_pretty(NULLIF(st_last2.size, 0)) as size2,
        pg_size_pretty(NULLIF(stat1.size_delta, 0)) as size_delta1,
        pg_size_pretty(NULLIF(stat2.size_delta, 0)) as size_delta2
    FROM tablespace_stats(sserver_id,start1_id,end1_id) stat1
        FULL OUTER JOIN tablespace_stats(sserver_id,start2_id,end2_id) stat2 USING (server_id,tablespaceid)
        LEFT OUTER JOIN v_sample_stat_tablespaces st_last1 ON
        (st_last1.server_id = stat1.server_id AND st_last1.sample_id = end1_id AND st_last1.tablespaceid = stat1.tablespaceid)
        LEFT OUTER JOIN v_sample_stat_tablespaces st_last2 ON
        (st_last2.server_id = stat2.server_id AND st_last2.sample_id = end2_id AND st_last2.tablespaceid = stat2.tablespaceid)
    ORDER BY COALESCE(stat1.tablespacename,stat2.tablespacename);

    r_result RECORD;
BEGIN
     -- Tablespace stats template
     jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Tablespace</th>'
            '<th>Path</th>'
            '<th>I</th>'
            '<th title="Tablespace size as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Tablespace size increment during report interval">Growth</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'ts_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr_mono}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['ts_tpl'],
            r_result.tablespacename,
            r_result.tablespacepath,
            r_result.size1,
            r_result.size_delta1,
            r_result.size2,
            r_result.size_delta2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;
    RETURN report;

END;
$$ LANGUAGE plpgsql;
/* ===== Tables stats functions ===== */

CREATE FUNCTION top_tables(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id integer,
    datid oid,
    relid oid,
    reltoastrelid oid,
    dbname name,
    tablespacename name,
    schemaname name,
    relname name,
    seq_scan bigint,
    seq_tup_read bigint,
    idx_scan bigint,
    idx_tup_fetch bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint,
    growth bigint,
    toastseq_scan bigint,
    toastseq_tup_read bigint,
    toastidx_scan bigint,
    toastidx_tup_fetch bigint,
    toastn_tup_ins bigint,
    toastn_tup_upd bigint,
    toastn_tup_del bigint,
    toastn_tup_hot_upd bigint,
    toastvacuum_count bigint,
    toastautovacuum_count bigint,
    toastanalyze_count bigint,
    toastautoanalyze_count bigint,
    toastgrowth bigint,
    relpagegrowth_bytes bigint,
    toastrelpagegrowth_bytes bigint,
    seqscan_bytes_relsize bigint,
    seqscan_bytes_relpages bigint,
    seqscan_relsize_avail boolean,
    t_seqscan_bytes_relsize bigint,
    t_seqscan_bytes_relpages bigint,
    t_seqscan_relsize_avail boolean
) SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        st.reltoastrelid,
        sample_db.datname AS dbname,
        tl.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.seq_tup_read)::bigint AS seq_tup_read,
        sum(st.idx_scan)::bigint AS idx_scan,
        sum(st.idx_tup_fetch)::bigint AS idx_tup_fetch,
        sum(st.n_tup_ins)::bigint AS n_tup_ins,
        sum(st.n_tup_upd)::bigint AS n_tup_upd,
        sum(st.n_tup_del)::bigint AS n_tup_del,
        sum(st.n_tup_hot_upd)::bigint AS n_tup_hot_upd,
        sum(st.vacuum_count)::bigint AS vacuum_count,
        sum(st.autovacuum_count)::bigint AS autovacuum_count,
        sum(st.analyze_count)::bigint AS analyze_count,
        sum(st.autoanalyze_count)::bigint AS autoanalyze_count,
        sum(st.relsize_diff)::bigint AS growth,
        sum(stt.seq_scan)::bigint AS toastseq_scan,
        sum(stt.seq_tup_read)::bigint AS toastseq_tup_read,
        sum(stt.idx_scan)::bigint AS toastidx_scan,
        sum(stt.idx_tup_fetch)::bigint AS toastidx_tup_fetch,
        sum(stt.n_tup_ins)::bigint AS toastn_tup_ins,
        sum(stt.n_tup_upd)::bigint AS toastn_tup_upd,
        sum(stt.n_tup_del)::bigint AS toastn_tup_del,
        sum(stt.n_tup_hot_upd)::bigint AS toastn_tup_hot_upd,
        sum(stt.vacuum_count)::bigint AS toastvacuum_count,
        sum(stt.autovacuum_count)::bigint AS toastautovacuum_count,
        sum(stt.analyze_count)::bigint AS toastanalyze_count,
        sum(stt.autoanalyze_count)::bigint AS toastautoanalyze_count,
        sum(stt.relsize_diff)::bigint AS toastgrowth,
        sum(st.relpages_bytes_diff)::bigint AS relpagegrowth_bytes,
        sum(stt.relpages_bytes_diff)::bigint AS toastrelpagegrowth_bytes,
        sum(st.seq_scan * st.relsize)::bigint AS seqscan_bytes_relsize,
        sum(st.seq_scan * st.relpages_bytes)::bigint AS seqscan_bytes_relpages,
        bool_and(COALESCE(st.seq_scan, 0) = 0 OR st.relsize IS NOT NULL) AS seqscan_relsize_avail,
        sum(stt.seq_scan * stt.relsize)::bigint AS t_seqscan_bytes_relsize,
        sum(stt.seq_scan * stt.relpages_bytes)::bigint AS t_seqscan_bytes_relpages,
        bool_and(COALESCE(stt.seq_scan, 0) = 0 OR stt.relsize IS NOT NULL) AS t_seqscan_relsize_avail
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
        JOIN tablespaces_list tl USING (server_id, tablespaceid)
        LEFT OUTER JOIN sample_stat_tables stt ON -- TOAST stats
          (st.server_id, st.sample_id, st.datid, st.reltoastrelid) =
          (stt.server_id, stt.sample_id, stt.datid, stt.relid)
    WHERE st.server_id = sserver_id AND st.relkind IN ('r','m') AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,st.reltoastrelid,sample_db.datname,tl.tablespacename,st.schemaname,st.relname
$$ LANGUAGE sql;

/* ===== Tables report functions ===== */
CREATE FUNCTION top_scan_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        reltoastrelid,
        NULLIF(seq_scan, 0) as seq_scan,
        NULLIF(best_seqscan_bytes, 0) as seq_scan_bytes,
        seqscan_relsize_avail,
        NULLIF(idx_scan, 0) as idx_scan,
        NULLIF(idx_tup_fetch, 0) as idx_tup_fetch,
        NULLIF(n_tup_ins, 0) as n_tup_ins,
        NULLIF(n_tup_upd, 0) as n_tup_upd,
        NULLIF(n_tup_del, 0) as n_tup_del,
        NULLIF(n_tup_hot_upd, 0) as n_tup_hot_upd,
        NULLIF(toastseq_scan, 0) as toastseq_scan,
        NULLIF(best_t_seqscan_bytes, 0) as toast_seq_scan_bytes,
        t_seqscan_relsize_avail,
        NULLIF(toastidx_scan, 0) as toastidx_scan,
        NULLIF(toastidx_tup_fetch, 0) as toastidx_tup_fetch,
        NULLIF(toastn_tup_ins, 0) as toastn_tup_ins,
        NULLIF(toastn_tup_upd, 0) as toastn_tup_upd,
        NULLIF(toastn_tup_del, 0) as toastn_tup_del,
        NULLIF(toastn_tup_hot_upd, 0) as toastn_tup_hot_upd
    FROM top_tables1 tt
    WHERE
      COALESCE(best_seqscan_bytes, 0) + COALESCE(best_t_seqscan_bytes, 0) > 0
    ORDER BY
      COALESCE(best_seqscan_bytes, 0) + COALESCE(best_t_seqscan_bytes, 0) DESC,
      tt.datid ASC,
      tt.relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Estimated number of bytes, fetched by sequential scans">~SeqBytes</th>'
            '<th title="Number of sequential scans initiated on this table">SeqScan</th>'
            '<th title="Number of index scans initiated on this table">IxScan</th>'
            '<th title="Number of live rows fetched by index scans">IxFet</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr {reltr}>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'rel_wtoast_tpl',
        '<tr {reltr}>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {toasttr}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);


    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        IF r_result.reltoastrelid IS NULL THEN
          report := report||format(
              jtab_tpl #>> ARRAY['rel_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              CASE WHEN r_result.seqscan_relsize_avail THEN
                  pg_size_pretty(r_result.seq_scan_bytes)
                ELSE
                  '['||pg_size_pretty(r_result.seq_scan_bytes)||']'
              END,
              r_result.seq_scan,
              r_result.idx_scan,
              r_result.idx_tup_fetch,
              r_result.n_tup_ins,
              r_result.n_tup_upd,
              r_result.n_tup_del,
              r_result.n_tup_hot_upd
          );
        ELSE
          report := report||format(
              jtab_tpl #>> ARRAY['rel_wtoast_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              CASE WHEN r_result.seqscan_relsize_avail THEN
                  pg_size_pretty(r_result.seq_scan_bytes)
                ELSE
                  '['||pg_size_pretty(r_result.seq_scan_bytes)||']'
              END,
              r_result.seq_scan,
              r_result.idx_scan,
              r_result.idx_tup_fetch,
              r_result.n_tup_ins,
              r_result.n_tup_upd,
              r_result.n_tup_del,
              r_result.n_tup_hot_upd,
              r_result.relname||'(TOAST)',
              CASE WHEN r_result.t_seqscan_relsize_avail THEN
                  pg_size_pretty(r_result.toast_seq_scan_bytes)
                ELSE
                  '['||pg_size_pretty(r_result.toast_seq_scan_bytes)||']'
              END,
              r_result.toastseq_scan,
              r_result.toastidx_scan,
              r_result.toastidx_tup_fetch,
              r_result.toastn_tup_ins,
              r_result.toastn_tup_upd,
              r_result.toastn_tup_del,
              r_result.toastn_tup_hot_upd
          );
        END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_scan_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) AS dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) AS schemaname,
        COALESCE(tbl1.relname,tbl2.relname) AS relname,
        NULLIF(tbl1.seq_scan, 0) AS seq_scan1,
        tbl1_seq_scan.approximated AS seq_scan_bytes_approximated1,
        NULLIF(tbl1_seq_scan.seq_scan_bytes, 0) AS seq_scan_bytes1,
        NULLIF(tbl1.idx_scan, 0) AS idx_scan1,
        NULLIF(tbl1.idx_tup_fetch, 0) AS idx_tup_fetch1,
        NULLIF(tbl1.toastseq_scan, 0) AS toastseq_scan1,
        toast1_seq_scan.approximated AS toastseq_scan_bytes_approximated1,
        NULLIF(toast1_seq_scan.seq_scan_bytes, 0) AS toastseq_scan_bytes1,
        NULLIF(tbl1.toastidx_scan, 0) AS toastidx_scan1,
        NULLIF(tbl1.toastidx_tup_fetch, 0) AS toastidx_tup_fetch1,
        NULLIF(tbl2.seq_scan, 0) AS seq_scan2,
        tbl2_seq_scan.approximated AS seq_scan_bytes_approximated2,
        NULLIF(tbl2_seq_scan.seq_scan_bytes, 0) AS seq_scan_bytes2,
        NULLIF(tbl2.idx_scan, 0) AS idx_scan2,
        NULLIF(tbl2.idx_tup_fetch, 0) AS idx_tup_fetch2,
        NULLIF(tbl2.toastseq_scan, 0) AS toastseq_scan2,
        toast2_seq_scan.approximated AS toastseq_scan_bytes_approximated2,
        NULLIF(toast2_seq_scan.seq_scan_bytes, 0) AS toastseq_scan_bytes2,
        NULLIF(tbl2.toastidx_scan, 0) AS toastidx_scan2,
        NULLIF(tbl2.toastidx_tup_fetch, 0) AS toastidx_tup_fetch2,
        row_number() over (ORDER BY
          COALESCE(tbl1_seq_scan.seq_scan_bytes, 0) + COALESCE(toast1_seq_scan.seq_scan_bytes, 0)
          DESC NULLS LAST) AS rn_seqpg1,
        row_number() over (ORDER BY
          COALESCE(tbl2_seq_scan.seq_scan_bytes, 0) + COALESCE(toast2_seq_scan.seq_scan_bytes, 0)
          DESC NULLS LAST) AS rn_seqpg2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id, datid, relid)
    LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        relid,
        round(sum(seq_scan * relsize))::bigint as seq_scan_bytes,
        count(nullif(relsize, 0)) != count(nullif(seq_scan, 0)) as approximated
      FROM sample_stat_tables
      WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
      GROUP BY
        server_id,
        datid,
        relid
    ) tbl1_seq_scan ON (tbl1.server_id,tbl1.datid,tbl1.relid) =
      (tbl1_seq_scan.server_id,tbl1_seq_scan.datid,tbl1_seq_scan.relid)
    LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        relid,
        round(sum(seq_scan * relsize))::bigint as seq_scan_bytes,
        count(nullif(relsize, 0)) != count(nullif(seq_scan, 0)) as approximated
      FROM sample_stat_tables
      WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
      GROUP BY
        server_id,
        datid,
        relid
    ) toast1_seq_scan ON (tbl1.server_id,tbl1.datid,tbl1.reltoastrelid) =
      (toast1_seq_scan.server_id,toast1_seq_scan.datid,toast1_seq_scan.relid)
    LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        relid,
        round(sum(seq_scan * relsize))::bigint as seq_scan_bytes,
        count(nullif(relsize, 0)) != count(nullif(seq_scan, 0)) as approximated
      FROM sample_stat_tables
      WHERE server_id = sserver_id AND sample_id BETWEEN start2_id + 1 AND end2_id
      GROUP BY
        server_id,
        datid,
        relid
    ) tbl2_seq_scan ON (tbl2.server_id,tbl2.datid,tbl2.relid) =
      (tbl2_seq_scan.server_id,tbl2_seq_scan.datid,tbl2_seq_scan.relid)
    LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        relid,
        round(sum(seq_scan * relsize))::bigint as seq_scan_bytes,
        count(nullif(relsize, 0)) != count(nullif(seq_scan, 0)) as approximated
      FROM sample_stat_tables
      WHERE server_id = sserver_id AND sample_id BETWEEN start2_id + 1 AND end2_id
      GROUP BY
        server_id,
        datid,
        relid
    ) toast2_seq_scan ON (tbl2.server_id,tbl2.datid,tbl2.reltoastrelid) =
      (toast2_seq_scan.server_id,toast2_seq_scan.datid,toast2_seq_scan.relid)
    WHERE COALESCE(tbl1_seq_scan.seq_scan_bytes, 0) +
      COALESCE(toast1_seq_scan.seq_scan_bytes, 0) +
      COALESCE(tbl2_seq_scan.seq_scan_bytes, 0) +
      COALESCE(toast2_seq_scan.seq_scan_bytes, 0) > 0
    ORDER BY
      COALESCE(tbl1_seq_scan.seq_scan_bytes, 0) +
      COALESCE(toast1_seq_scan.seq_scan_bytes, 0) +
      COALESCE(tbl2_seq_scan.seq_scan_bytes, 0) +
      COALESCE(toast2_seq_scan.seq_scan_bytes, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_seqpg1,
        rn_seqpg2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="4">Table</th>'
            '<th colspan="4">TOAST</th>'
          '</tr>'
          '<tr>'
            '<th title="Estimated number of blocks, fetched by sequential scans">~SeqBytes</th>'
            '<th title="Number of sequential scans initiated on this table">SeqScan</th>'
            '<th title="Number of index scans initiated on this table">IxScan</th>'
            '<th title="Number of live rows fetched by index scans">IxFet</th>'
            '<th title="Estimated number of blocks, fetched by sequential scans">~SeqBytes</th>'
            '<th title="Number of sequential scans initiated on this table">SeqScan</th>'
            '<th title="Number of index scans initiated on this table">IxScan</th>'
            '<th title="Number of live rows fetched by index scans">IxFet</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            CASE WHEN r_result.seq_scan_bytes_approximated1 THEN '~'
              ELSE ''
            END||pg_size_pretty(r_result.seq_scan_bytes1),
            r_result.seq_scan1,
            r_result.idx_scan1,
            r_result.idx_tup_fetch1,
            CASE WHEN r_result.toastseq_scan_bytes_approximated1 THEN '~'
              ELSE ''
            END||pg_size_pretty(r_result.toastseq_scan_bytes1),
            r_result.toastseq_scan1,
            r_result.toastidx_scan1,
            r_result.toastidx_tup_fetch1,
            CASE WHEN r_result.seq_scan_bytes_approximated2 THEN '~'
              ELSE ''
            END||pg_size_pretty(r_result.seq_scan_bytes2),
            r_result.seq_scan2,
            r_result.idx_scan2,
            r_result.idx_tup_fetch2,
            CASE WHEN r_result.toastseq_scan_bytes_approximated2 THEN '~'
              ELSE ''
            END||pg_size_pretty(r_result.toastseq_scan_bytes2),
            r_result.toastseq_scan2,
            r_result.toastidx_scan2,
            r_result.toastidx_tup_fetch2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_dml_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        reltoastrelid,
        NULLIF(seq_scan, 0) as seq_scan,
        NULLIF(seq_tup_read, 0) as seq_tup_read,
        NULLIF(idx_scan, 0) as idx_scan,
        NULLIF(idx_tup_fetch, 0) as idx_tup_fetch,
        NULLIF(n_tup_ins, 0) as n_tup_ins,
        NULLIF(n_tup_upd, 0) as n_tup_upd,
        NULLIF(n_tup_del, 0) as n_tup_del,
        NULLIF(n_tup_hot_upd, 0) as n_tup_hot_upd,
        NULLIF(toastseq_scan, 0) as toastseq_scan,
        NULLIF(toastseq_tup_read, 0) as toastseq_tup_read,
        NULLIF(toastidx_scan, 0) as toastidx_scan,
        NULLIF(toastidx_tup_fetch, 0) as toastidx_tup_fetch,
        NULLIF(toastn_tup_ins, 0) as toastn_tup_ins,
        NULLIF(toastn_tup_upd, 0) as toastn_tup_upd,
        NULLIF(toastn_tup_del, 0) as toastn_tup_del,
        NULLIF(toastn_tup_hot_upd, 0) as toastn_tup_hot_upd
    FROM top_tables1
    WHERE COALESCE(n_tup_ins, 0) + COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) +
      COALESCE(toastn_tup_ins, 0) + COALESCE(toastn_tup_upd, 0) + COALESCE(toastn_tup_del, 0) > 0
    ORDER BY COALESCE(n_tup_ins, 0) + COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) +
      COALESCE(toastn_tup_ins, 0) + COALESCE(toastn_tup_upd, 0) + COALESCE(toastn_tup_del, 0) DESC,
      datid ASC,
      relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Number of sequential scans initiated on this table">SeqScan</th>'
            '<th title="Number of live rows fetched by sequential scans">SeqFet</th>'
            '<th title="Number of index scans initiated on this table">IxScan</th>'
            '<th title="Number of live rows fetched by index scans">IxFet</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr {reltr}>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'rel_wtoast_tpl',
        '<tr {reltr}>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {toasttr}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        IF r_result.reltoastrelid IS NULL THEN
          report := report||format(
              jtab_tpl #>> ARRAY['rel_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              r_result.n_tup_ins,
              r_result.n_tup_upd,
              r_result.n_tup_del,
              r_result.n_tup_hot_upd,
              r_result.seq_scan,
              r_result.seq_tup_read,
              r_result.idx_scan,
              r_result.idx_tup_fetch
          );
        ELSE
          report := report||format(
              jtab_tpl #>> ARRAY['rel_wtoast_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              r_result.n_tup_ins,
              r_result.n_tup_upd,
              r_result.n_tup_del,
              r_result.n_tup_hot_upd,
              r_result.seq_scan,
              r_result.seq_tup_read,
              r_result.idx_scan,
              r_result.idx_tup_fetch,
              r_result.relname||'(TOAST)',
              r_result.toastn_tup_ins,
              r_result.toastn_tup_upd,
              r_result.toastn_tup_del,
              r_result.toastn_tup_hot_upd,
              r_result.toastseq_scan,
              r_result.toastseq_tup_read,
              r_result.toastidx_scan,
              r_result.toastidx_tup_fetch
          );
        END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_dml_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) AS dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) AS schemaname,
        COALESCE(tbl1.relname,tbl2.relname) AS relname,
        NULLIF(tbl1.n_tup_ins, 0) AS n_tup_ins1,
        NULLIF(tbl1.n_tup_upd, 0) AS n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) AS n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) AS n_tup_hot_upd1,
        NULLIF(tbl1.toastn_tup_ins, 0) AS toastn_tup_ins1,
        NULLIF(tbl1.toastn_tup_upd, 0) AS toastn_tup_upd1,
        NULLIF(tbl1.toastn_tup_del, 0) AS toastn_tup_del1,
        NULLIF(tbl1.toastn_tup_hot_upd, 0) AS toastn_tup_hot_upd1,
        NULLIF(tbl2.n_tup_ins, 0) AS n_tup_ins2,
        NULLIF(tbl2.n_tup_upd, 0) AS n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) AS n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) AS n_tup_hot_upd2,
        NULLIF(tbl2.toastn_tup_ins, 0) AS toastn_tup_ins2,
        NULLIF(tbl2.toastn_tup_upd, 0) AS toastn_tup_upd2,
        NULLIF(tbl2.toastn_tup_del, 0) AS toastn_tup_del2,
        NULLIF(tbl2.toastn_tup_hot_upd, 0) AS toastn_tup_hot_upd2,
        row_number() OVER (ORDER BY COALESCE(tbl1.n_tup_ins, 0) + COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
          COALESCE(tbl1.toastn_tup_ins, 0) + COALESCE(tbl1.toastn_tup_upd, 0) + COALESCE(tbl1.toastn_tup_del, 0) DESC NULLS LAST) AS rn_dml1,
        row_number() OVER (ORDER BY COALESCE(tbl2.n_tup_ins, 0) + COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) +
          COALESCE(tbl2.toastn_tup_ins, 0) + COALESCE(tbl2.toastn_tup_upd, 0) + COALESCE(tbl2.toastn_tup_del, 0) DESC NULLS LAST) AS rn_dml2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id, datid, relid)
    WHERE COALESCE(tbl1.n_tup_ins, 0) + COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
        COALESCE(tbl1.toastn_tup_ins, 0) + COALESCE(tbl1.toastn_tup_upd, 0) + COALESCE(tbl1.toastn_tup_del, 0) +
        COALESCE(tbl2.n_tup_ins, 0) + COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) +
        COALESCE(tbl2.toastn_tup_ins, 0) + COALESCE(tbl2.toastn_tup_upd, 0) + COALESCE(tbl2.toastn_tup_del, 0) > 0
    ORDER BY COALESCE(tbl1.n_tup_ins, 0) + COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
          COALESCE(tbl1.toastn_tup_ins, 0) + COALESCE(tbl1.toastn_tup_upd, 0) + COALESCE(tbl1.toastn_tup_del, 0) +
          COALESCE(tbl2.n_tup_ins, 0) + COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) +
          COALESCE(tbl2.toastn_tup_ins, 0) + COALESCE(tbl2.toastn_tup_upd, 0) + COALESCE(tbl2.toastn_tup_del, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_dml1,
        rn_dml2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="4">Table</th>'
            '<th colspan="4">TOAST</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.toastn_tup_ins1,
            r_result.toastn_tup_upd1,
            r_result.toastn_tup_del1,
            r_result.toastn_tup_hot_upd1,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2,
            r_result.toastn_tup_ins2,
            r_result.toastn_tup_upd2,
            r_result.toastn_tup_del2,
            r_result.toastn_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_upd_vac_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        reltoastrelid,
        NULLIF(n_tup_upd, 0) as n_tup_upd,
        NULLIF(n_tup_del, 0) as n_tup_del,
        NULLIF(n_tup_hot_upd, 0) as n_tup_hot_upd,
        NULLIF(vacuum_count, 0) as vacuum_count,
        NULLIF(autovacuum_count, 0) as autovacuum_count,
        NULLIF(analyze_count, 0) as analyze_count,
        NULLIF(autoanalyze_count, 0) as autoanalyze_count,
        NULLIF(toastn_tup_upd, 0) as toastn_tup_upd,
        NULLIF(toastn_tup_del, 0) as toastn_tup_del,
        NULLIF(toastn_tup_hot_upd, 0) as toastn_tup_hot_upd,
        NULLIF(toastvacuum_count, 0) as toastvacuum_count,
        NULLIF(toastautovacuum_count, 0) as toastautovacuum_count,
        NULLIF(toastanalyze_count, 0) as toastanalyze_count,
        NULLIF(toastautoanalyze_count, 0) as toastautoanalyze_count
    FROM top_tables1
    WHERE COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) +
      COALESCE(toastn_tup_upd, 0) + COALESCE(toastn_tup_del, 0) > 0
    ORDER BY COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) +
      COALESCE(toastn_tup_upd, 0) + COALESCE(toastn_tup_del, 0) DESC,
      datid ASC,
      relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of times this table has been manually vacuumed (not counting VACUUM FULL)">Vacuum</th>'
            '<th title="Number of times this table has been vacuumed by the autovacuum daemon">AutoVacuum</th>'
            '<th title="Number of times this table has been manually analyzed">Analyze</th>'
            '<th title="Number of times this table has been analyzed by the autovacuum daemon">AutoAnalyze</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr {reltr}>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'rel_wtoast_tpl',
        '<tr {reltr}>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {toasttr}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        IF r_result.reltoastrelid IS NULL THEN
          report := report||format(
              jtab_tpl #>> ARRAY['rel_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              r_result.n_tup_upd,
              r_result.n_tup_hot_upd,
              r_result.n_tup_del,
              r_result.vacuum_count,
              r_result.autovacuum_count,
              r_result.analyze_count,
              r_result.autoanalyze_count
          );
        ELSE
          report := report||format(
              jtab_tpl #>> ARRAY['rel_wtoast_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              r_result.n_tup_upd,
              r_result.n_tup_hot_upd,
              r_result.n_tup_del,
              r_result.vacuum_count,
              r_result.autovacuum_count,
              r_result.analyze_count,
              r_result.autoanalyze_count,
              r_result.relname||'(TOAST)',
              r_result.toastn_tup_upd,
              r_result.toastn_tup_hot_upd,
              r_result.toastn_tup_del,
              r_result.toastvacuum_count,
              r_result.toastautovacuum_count,
              r_result.toastanalyze_count,
              r_result.toastautoanalyze_count
          );
        END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_upd_vac_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        NULLIF(tbl1.n_tup_upd, 0) as n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) as n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) as n_tup_hot_upd1,
        NULLIF(tbl1.vacuum_count, 0) as vacuum_count1,
        NULLIF(tbl1.autovacuum_count, 0) as autovacuum_count1,
        NULLIF(tbl1.analyze_count, 0) as analyze_count1,
        NULLIF(tbl1.autoanalyze_count, 0) as autoanalyze_count1,
        NULLIF(tbl2.n_tup_upd, 0) as n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) as n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) as n_tup_hot_upd2,
        NULLIF(tbl2.vacuum_count, 0) as vacuum_count2,
        NULLIF(tbl2.autovacuum_count, 0) as autovacuum_count2,
        NULLIF(tbl2.analyze_count, 0) as analyze_count2,
        NULLIF(tbl2.autoanalyze_count, 0) as autoanalyze_count2,
        row_number() OVER (ORDER BY COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) DESC NULLS LAST) as rn_vactpl1,
        row_number() OVER (ORDER BY COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) DESC NULLS LAST) as rn_vactpl2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id, datid, relid)
    WHERE COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
          COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) > 0
    ORDER BY COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
          COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_vactpl1,
        rn_vactpl2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>I</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of times this table has been manually vacuumed (not counting VACUUM FULL)">Vacuum</th>'
            '<th title="Number of times this table has been vacuumed by the autovacuum daemon">AutoVacuum</th>'
            '<th title="Number of times this table has been manually analyzed">Analyze</th>'
            '<th title="Number of times this table has been analyzed by the autovacuum daemon">AutoAnalyze</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_upd1,
            r_result.n_tup_hot_upd1,
            r_result.n_tup_del1,
            r_result.vacuum_count1,
            r_result.autovacuum_count1,
            r_result.analyze_count1,
            r_result.autoanalyze_count1,
            r_result.n_tup_upd2,
            r_result.n_tup_hot_upd2,
            r_result.n_tup_del2,
            r_result.vacuum_count2,
            r_result.autovacuum_count2,
            r_result.analyze_count2,
            r_result.autoanalyze_count2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_growth_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(end1_id integer, topn integer) FOR
    SELECT
        dbname,
        top.tablespacename,
        top.schemaname,
        top.relname,
        top.reltoastrelid,
        NULLIF(top.n_tup_ins, 0) as n_tup_ins,
        NULLIF(top.n_tup_upd, 0) as n_tup_upd,
        NULLIF(top.n_tup_del, 0) as n_tup_del,
        NULLIF(top.n_tup_hot_upd, 0) as n_tup_hot_upd,
        NULLIF(top.best_growth, 0) AS growth,
        NULLIF(st_last.relsize, 0) AS relsize,
        NULLIF(st_last.relpages_bytes, 0) AS relpages_bytes,
        NULLIF(top.toastn_tup_ins, 0) as toastn_tup_ins,
        NULLIF(top.toastn_tup_upd, 0) as toastn_tup_upd,
        NULLIF(top.toastn_tup_del, 0) as toastn_tup_del,
        NULLIF(top.toastn_tup_hot_upd, 0) as toastn_tup_hot_upd,
        NULLIF(top.best_toastgrowth, 0) AS toastgrowth,
        NULLIF(stt_last.relsize, 0) AS toastrelsize,
        NULLIF(stt_last.relpages_bytes, 0) AS toastrelpages_bytes,
        top.relsize_growth_avail,
        top.relsize_toastgrowth_avail
    FROM top_tables1 top
        JOIN sample_stat_tables st_last
          USING (server_id, datid, relid)
        LEFT OUTER JOIN sample_stat_tables stt_last ON
          (stt_last.server_id, stt_last.datid, stt_last.relid, stt_last.sample_id) =
          (top.server_id, top.datid, top.reltoastrelid, end1_id)
    WHERE st_last.sample_id = end1_id AND
      COALESCE(top.best_growth,0) + COALESCE(top.best_toastgrowth,0) > 0
    ORDER BY
      COALESCE(top.best_growth,0) + COALESCE(top.best_toastgrowth,0) DESC,
      top.datid ASC,
      top.relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Table size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Table size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr {reltr}>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'rel_wtoast_tpl',
        '<tr {reltr}>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {toasttr}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
      IF r_result.reltoastrelid IS NULL THEN
        report := report||format(
            jtab_tpl #>> ARRAY['rel_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            COALESCE(
              pg_size_pretty(r_result.relsize),
              '['||pg_size_pretty(r_result.relpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail
              THEN pg_size_pretty(r_result.growth)
              ELSE '['||pg_size_pretty(r_result.growth)||']'
            END,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd
        );
      ELSE
        report := report||format(
            jtab_tpl #>> ARRAY['rel_wtoast_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            COALESCE(
              pg_size_pretty(r_result.relsize),
              '['||pg_size_pretty(r_result.relpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail
              THEN pg_size_pretty(r_result.growth)
              ELSE '['||pg_size_pretty(r_result.growth)||']'
            END,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd,
            r_result.relname||'(TOAST)',
            COALESCE(
              pg_size_pretty(r_result.toastrelsize),
              '['||pg_size_pretty(r_result.toastrelpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_toastgrowth_avail
              THEN pg_size_pretty(r_result.toastgrowth)
              ELSE '['||pg_size_pretty(r_result.toastgrowth)||']'
            END,
            r_result.toastn_tup_ins,
            r_result.toastn_tup_upd,
            r_result.toastn_tup_del,
            r_result.toastn_tup_hot_upd
        );
      END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_growth_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(end1_id integer,
      end2_id integer, topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        NULLIF(tbl1.n_tup_ins, 0) as n_tup_ins1,
        NULLIF(tbl1.n_tup_upd, 0) as n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) as n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) as n_tup_hot_upd1,
        NULLIF(tbl1.best_growth, 0) as growth1,
        NULLIF(st_last1.relsize, 0) as relsize1,
        NULLIF(st_last1.relpages_bytes, 0) AS relpages_bytes1,
        NULLIF(tbl1.toastn_tup_ins, 0) as toastn_tup_ins1,
        NULLIF(tbl1.toastn_tup_upd, 0) as toastn_tup_upd1,
        NULLIF(tbl1.toastn_tup_del, 0) as toastn_tup_del1,
        NULLIF(tbl1.toastn_tup_hot_upd, 0) as toastn_tup_hot_upd1,
        NULLIF(tbl1.best_toastgrowth, 0) AS toastgrowth1,
        NULLIF(stt_last1.relsize, 0) AS toastrelsize1,
        NULLIF(stt_last1.relpages_bytes, 0) AS toastrelpages_bytes1,
        NULLIF(tbl2.n_tup_ins, 0) as n_tup_ins2,
        NULLIF(tbl2.n_tup_upd, 0) as n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) as n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) as n_tup_hot_upd2,
        NULLIF(tbl2.best_growth, 0) as growth2,
        NULLIF(st_last2.relsize, 0) as relsize2,
        NULLIF(st_last2.relpages_bytes, 0) AS relpages_bytes2,
        NULLIF(tbl2.toastn_tup_ins, 0) as toastn_tup_ins2,
        NULLIF(tbl2.toastn_tup_upd, 0) as toastn_tup_upd2,
        NULLIF(tbl2.toastn_tup_del, 0) as toastn_tup_del2,
        NULLIF(tbl2.toastn_tup_hot_upd, 0) as toastn_tup_hot_upd2,
        NULLIF(tbl2.best_toastgrowth, 0) AS toastgrowth2,
        NULLIF(stt_last2.relsize, 0) AS toastrelsize2,
        NULLIF(stt_last2.relpages_bytes, 0) AS toastrelpages_bytes2,
        tbl1.relsize_growth_avail as relsize_growth_avail1,
        tbl1.relsize_toastgrowth_avail as relsize_toastgrowth_avail1,
        tbl2.relsize_growth_avail as relsize_growth_avail2,
        tbl2.relsize_toastgrowth_avail as relsize_toastgrowth_avail2,
        row_number() OVER (ORDER BY COALESCE(tbl1.best_growth, 0) + COALESCE(tbl1.best_toastgrowth, 0) DESC NULLS LAST) as rn_growth1,
        row_number() OVER (ORDER BY COALESCE(tbl2.best_growth, 0) + COALESCE(tbl2.best_toastgrowth, 0) DESC NULLS LAST) as rn_growth2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id,datid,relid)
        LEFT OUTER JOIN sample_stat_tables st_last1 ON
          (st_last1.server_id, st_last1.datid, st_last1.relid, st_last1.sample_id) =
          (tbl1.server_id, tbl1.datid, tbl1.relid, end1_id)
        LEFT OUTER JOIN sample_stat_tables st_last2 ON
          (st_last2.server_id, st_last2.datid, st_last2.relid, st_last2.sample_id) =
          (tbl2.server_id, tbl2.datid, tbl2.relid, end2_id)
        -- join toast tables last sample stats (to get relsize)
        LEFT OUTER JOIN sample_stat_tables stt_last1 ON
          (stt_last1.server_id, stt_last1.datid, stt_last1.relid, stt_last1.sample_id) =
          (st_last1.server_id, st_last1.datid, tbl1.reltoastrelid, st_last1.sample_id)
        LEFT OUTER JOIN sample_stat_tables stt_last2 ON
          (stt_last2.server_id, stt_last2.datid, stt_last2.relid, stt_last2.sample_id) =
          (st_last2.server_id, st_last2.datid, tbl2.reltoastrelid, st_last2.sample_id)
    WHERE COALESCE(tbl1.best_growth, 0) + COALESCE(tbl1.best_toastgrowth, 0) +
      COALESCE(tbl2.best_growth, 0) + COALESCE(tbl2.best_toastgrowth, 0) > 0
    ORDER BY COALESCE(tbl1.best_growth, 0) + COALESCE(tbl1.best_toastgrowth, 0) +
      COALESCE(tbl2.best_growth, 0) + COALESCE(tbl2.best_toastgrowth, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_growth1,
        rn_growth2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="6">Table</th>'
            '<th colspan="6">TOAST</th>'
          '</tr>'
          '<tr>'
            '<th title="Table size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Table size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Table size, as it was at the moment of last sample in report interval (TOAST)">Size</th>'
            '<th title="Table size increment during report interval (TOAST)">Growth</th>'
            '<th title="Number of rows inserted (TOAST)">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows) (TOAST)">Upd</th>'
            '<th title="Number of rows deleted (TOAST)">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required) (TOAST)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            COALESCE(
              pg_size_pretty(r_result.relsize1),
              '['||pg_size_pretty(r_result.relpages_bytes1)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail1
              THEN pg_size_pretty(r_result.growth1)
              ELSE '['||pg_size_pretty(r_result.growth1)||']'
            END,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            COALESCE(
              pg_size_pretty(r_result.toastrelsize1),
              '['||pg_size_pretty(r_result.toastrelpages_bytes1)||']'
            ),
            CASE WHEN r_result.relsize_toastgrowth_avail1
              THEN pg_size_pretty(r_result.toastgrowth1)
              ELSE '['||pg_size_pretty(r_result.toastgrowth1)||']'
            END,
            r_result.toastn_tup_ins1,
            r_result.toastn_tup_upd1,
            r_result.toastn_tup_del1,
            r_result.toastn_tup_hot_upd1,
            COALESCE(
              pg_size_pretty(r_result.relsize2),
              '['||pg_size_pretty(r_result.relpages_bytes2)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail2
              THEN pg_size_pretty(r_result.growth2)
              ELSE '['||pg_size_pretty(r_result.growth2)||']'
            END,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2,
            COALESCE(
              pg_size_pretty(r_result.toastrelsize2),
              '['||pg_size_pretty(r_result.toastrelpages_bytes2)||']'
            ),
            CASE WHEN r_result.relsize_toastgrowth_avail2
              THEN pg_size_pretty(r_result.toastgrowth2)
              ELSE '['||pg_size_pretty(r_result.toastgrowth2)||']'
            END,
            r_result.toastn_tup_ins2,
            r_result.toastn_tup_upd2,
            r_result.toastn_tup_del2,
            r_result.toastn_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_vacuumed_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        top.tablespacename,
        top.schemaname,
        top.relname,
        NULLIF(top.vacuum_count, 0) as vacuum_count,
        NULLIF(top.autovacuum_count, 0) as autovacuum_count,
        NULLIF(top.n_tup_ins, 0) as n_tup_ins,
        NULLIF(top.n_tup_upd, 0) as n_tup_upd,
        NULLIF(top.n_tup_del, 0) as n_tup_del,
        NULLIF(top.n_tup_hot_upd, 0) as n_tup_hot_upd
    FROM top_tables1 top
    WHERE COALESCE(top.vacuum_count, 0) + COALESCE(top.autovacuum_count, 0) > 0
    ORDER BY COALESCE(top.vacuum_count, 0) + COALESCE(top.autovacuum_count, 0) DESC,
      top.datid ASC,
      top.relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Number of times this table has been manually vacuumed (not counting VACUUM FULL)">Vacuum count</th>'
            '<th title="Number of times this table has been vacuumed by the autovacuum daemon">Autovacuum count</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
    );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
      report := report||format(
          jtab_tpl #>> ARRAY['rel_tpl'],
          r_result.dbname,
          r_result.tablespacename,
          r_result.schemaname,
          r_result.relname,
          r_result.vacuum_count,
          r_result.autovacuum_count,
          r_result.n_tup_ins,
          r_result.n_tup_upd,
          r_result.n_tup_del,
          r_result.n_tup_hot_upd
      );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_vacuumed_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        NULLIF(tbl1.vacuum_count, 0) as vacuum_count1,
        NULLIF(tbl1.autovacuum_count, 0) as autovacuum_count1,
        NULLIF(tbl1.n_tup_ins, 0) as n_tup_ins1,
        NULLIF(tbl1.n_tup_upd, 0) as n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) as n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) as n_tup_hot_upd1,
        NULLIF(tbl2.vacuum_count, 0) as vacuum_count2,
        NULLIF(tbl2.autovacuum_count, 0) as autovacuum_count2,
        NULLIF(tbl2.n_tup_ins, 0) as n_tup_ins2,
        NULLIF(tbl2.n_tup_upd, 0) as n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) as n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) as n_tup_hot_upd2,
        row_number() OVER (ORDER BY COALESCE(tbl1.vacuum_count, 0) + COALESCE(tbl1.autovacuum_count, 0) DESC) as rn_vacuum1,
        row_number() OVER (ORDER BY COALESCE(tbl2.vacuum_count, 0) + COALESCE(tbl2.autovacuum_count, 0) DESC) as rn_vacuum2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id,datid,relid)
    WHERE COALESCE(tbl1.vacuum_count, 0) + COALESCE(tbl1.autovacuum_count, 0) +
          COALESCE(tbl2.vacuum_count, 0) + COALESCE(tbl2.autovacuum_count, 0) > 0
    ORDER BY COALESCE(tbl1.vacuum_count, 0) + COALESCE(tbl1.autovacuum_count, 0) +
          COALESCE(tbl2.vacuum_count, 0) + COALESCE(tbl2.autovacuum_count, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_vacuum1,
        rn_vacuum2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>I</th>'
            '<th title="Number of times this table has been manually vacuumed (not counting VACUUM FULL)">Vacuum count</th>'
            '<th title="Number of times this table has been vacuumed by the autovacuum daemon">Autovacuum count</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.vacuum_count1,
            r_result.autovacuum_count1,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.vacuum_count2,
            r_result.autovacuum_count2,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_analyzed_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        top.tablespacename,
        top.schemaname,
        top.relname,
        NULLIF(top.analyze_count, 0) as analyze_count,
        NULLIF(top.autoanalyze_count, 0) as autoanalyze_count,
        NULLIF(top.n_tup_ins, 0) as n_tup_ins,
        NULLIF(top.n_tup_upd, 0) as n_tup_upd,
        NULLIF(top.n_tup_del, 0) as n_tup_del,
        NULLIF(top.n_tup_hot_upd, 0) as n_tup_hot_upd
    FROM top_tables1 top
    WHERE COALESCE(top.analyze_count, 0) + COALESCE(top.autoanalyze_count, 0) > 0
    ORDER BY COALESCE(top.analyze_count, 0) + COALESCE(top.autoanalyze_count, 0) DESC,
      top.datid ASC,
      top.relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Number of times this table has been manually analyzed">Analyze count</th>'
            '<th title="Number of times this table has been analyzed by the autovacuum daemon">Autoanalyze count</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
    );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
      report := report||format(
          jtab_tpl #>> ARRAY['rel_tpl'],
          r_result.dbname,
          r_result.tablespacename,
          r_result.schemaname,
          r_result.relname,
          r_result.analyze_count,
          r_result.autoanalyze_count,
          r_result.n_tup_ins,
          r_result.n_tup_upd,
          r_result.n_tup_del,
          r_result.n_tup_hot_upd
      );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_analyzed_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        NULLIF(tbl1.analyze_count, 0) as analyze_count1,
        NULLIF(tbl1.autoanalyze_count, 0) as autoanalyze_count1,
        NULLIF(tbl1.n_tup_ins, 0) as n_tup_ins1,
        NULLIF(tbl1.n_tup_upd, 0) as n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) as n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) as n_tup_hot_upd1,
        NULLIF(tbl2.analyze_count, 0) as analyze_count2,
        NULLIF(tbl2.autoanalyze_count, 0) as autoanalyze_count2,
        NULLIF(tbl2.n_tup_ins, 0) as n_tup_ins2,
        NULLIF(tbl2.n_tup_upd, 0) as n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) as n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) as n_tup_hot_upd2,
        row_number() OVER (ORDER BY COALESCE(tbl1.analyze_count, 0) + COALESCE(tbl1.autoanalyze_count, 0) DESC) as rn_analyze1,
        row_number() OVER (ORDER BY COALESCE(tbl2.analyze_count, 0) + COALESCE(tbl2.autoanalyze_count, 0) DESC) as rn_analyze2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id,datid,relid)
    WHERE COALESCE(tbl1.analyze_count, 0) + COALESCE(tbl1.autoanalyze_count, 0) +
          COALESCE(tbl2.analyze_count, 0) + COALESCE(tbl2.autoanalyze_count, 0) > 0
    ORDER BY COALESCE(tbl1.analyze_count, 0) + COALESCE(tbl1.autoanalyze_count, 0) +
          COALESCE(tbl2.analyze_count, 0) + COALESCE(tbl2.autoanalyze_count, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_analyze1,
        rn_analyze2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>I</th>'
            '<th title="Number of times this table has been manually analyzed">Analyze count</th>'
            '<th title="Number of times this table has been analyzed by the autovacuum daemon">Autoanalyze count</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.analyze_count1,
            r_result.autoanalyze_count1,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.analyze_count2,
            r_result.autoanalyze_count2,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
/* ===== Top IO objects ===== */

CREATE FUNCTION top_io_tables(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id                     integer,
    datid                       oid,
    relid                       oid,
    dbname                      name,
    tablespacename              name,
    schemaname                  name,
    relname                     name,
    heap_blks_read              bigint,
    heap_blks_read_pct          numeric,
    heap_blks_fetch             bigint,
    heap_blks_proc_pct          numeric,
    idx_blks_read               bigint,
    idx_blks_read_pct           numeric,
    idx_blks_fetch              bigint,
    idx_blks_fetch_pct           numeric,
    toast_blks_read             bigint,
    toast_blks_read_pct         numeric,
    toast_blks_fetch            bigint,
    toast_blks_fetch_pct        numeric,
    tidx_blks_read              bigint,
    tidx_blks_read_pct          numeric,
    tidx_blks_fetch             bigint,
    tidx_blks_fetch_pct         numeric,
    seq_scan                    bigint,
    idx_scan                    bigint
) SET search_path=@extschema@ AS $$
    WITH total AS (SELECT
      COALESCE(sum(heap_blks_read), 0) + COALESCE(sum(idx_blks_read), 0) AS total_blks_read,
      COALESCE(sum(heap_blks_read), 0) + COALESCE(sum(idx_blks_read), 0) +
      COALESCE(sum(heap_blks_hit), 0) + COALESCE(sum(idx_blks_hit), 0) AS total_blks_fetch
    FROM sample_stat_tables_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    )
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        tablespaces_list.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.heap_blks_read)::bigint AS heap_blks_read,
        sum(st.heap_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS heap_blks_read_pct,
        COALESCE(sum(st.heap_blks_read), 0)::bigint + COALESCE(sum(st.heap_blks_hit), 0)::bigint AS heap_blks_fetch,
        (COALESCE(sum(st.heap_blks_read), 0) + COALESCE(sum(st.heap_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS heap_blks_proc_pct,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.idx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS idx_blks_read_pct,
        COALESCE(sum(st.idx_blks_read), 0)::bigint + COALESCE(sum(st.idx_blks_hit), 0)::bigint AS idx_blks_fetch,
        (COALESCE(sum(st.idx_blks_read), 0) + COALESCE(sum(st.idx_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS idx_blks_fetch_pct,
        sum(st.toast_blks_read)::bigint AS toast_blks_read,
        sum(st.toast_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS toast_blks_read_pct,
        COALESCE(sum(st.toast_blks_read), 0)::bigint + COALESCE(sum(st.toast_blks_hit), 0)::bigint AS toast_blks_fetch,
        (COALESCE(sum(st.toast_blks_read), 0) + COALESCE(sum(st.toast_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS toast_blks_fetch_pct,
        sum(st.tidx_blks_read)::bigint AS tidx_blks_read,
        sum(st.tidx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS tidx_blks_read_pct,
        COALESCE(sum(st.tidx_blks_read), 0)::bigint + COALESCE(sum(st.tidx_blks_hit), 0)::bigint AS tidx_blks_fetch,
        (COALESCE(sum(st.tidx_blks_read), 0) + COALESCE(sum(st.tidx_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS tidx_blks_fetch_pct,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.idx_scan)::bigint AS idx_scan
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
        JOIN tablespaces_list USING(server_id,tablespaceid)
        CROSS JOIN total
    WHERE st.server_id = sserver_id
      AND st.relkind IN ('r','m')
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,sample_db.datname,tablespaces_list.tablespacename, st.schemaname,st.relname
    HAVING min(sample_db.stats_reset) = max(sample_db.stats_reset)
$$ LANGUAGE sql;

CREATE FUNCTION top_io_indexes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    datid               oid,
    relid               oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelid          oid,
    indexrelname        name,
    idx_scan            bigint,
    idx_blks_read       bigint,
    idx_blks_read_pct   numeric,
    idx_blks_hit_pct    numeric,
    idx_blks_fetch  bigint,
    idx_blks_fetch_pct   numeric
) SET search_path=@extschema@ AS $$
    WITH total AS (SELECT
      COALESCE(sum(heap_blks_read)) + COALESCE(sum(idx_blks_read)) AS total_blks_read,
      COALESCE(sum(heap_blks_read)) + COALESCE(sum(idx_blks_read)) +
      COALESCE(sum(heap_blks_hit)) + COALESCE(sum(idx_blks_hit)) AS total_blks_fetch
    FROM sample_stat_tables_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    )
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name AS relname,
        st.indexrelid,
        st.indexrelname,
        sum(st.idx_scan)::bigint AS idx_scan,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.idx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS idx_blks_read_pct,
        sum(st.idx_blks_hit) * 100 / NULLIF(COALESCE(sum(st.idx_blks_hit), 0) + COALESCE(sum(st.idx_blks_read), 0), 0) AS idx_blks_hit_pct,
        COALESCE(sum(st.idx_blks_read), 0)::bigint + COALESCE(sum(st.idx_blks_hit), 0)::bigint AS idx_blks_fetch,
        (COALESCE(sum(st.idx_blks_read), 0) + COALESCE(sum(st.idx_blks_hit), 0)) * 100 / NULLIF(min(total_blks_fetch), 0) AS idx_blks_fetch_pct
    FROM v_sample_stat_indexes st
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
        JOIN tablespaces_list ON  (st.server_id=tablespaces_list.server_id AND st.tablespaceid=tablespaces_list.tablespaceid)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON (st.server_id = mtbl.server_id AND st.datid = mtbl.datid AND st.relid = mtbl.reltoastrelid)
        CROSS JOIN total
    WHERE st.server_id = sserver_id AND NOT sample_db.datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,sample_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname), COALESCE(mtbl.relname||'(TOAST)',st.relname),
      st.schemaname,st.relname,tablespaces_list.tablespacename, st.indexrelid,st.indexrelname
    HAVING min(sample_db.stats_reset) = max(sample_db.stats_reset)
$$ LANGUAGE sql;

CREATE FUNCTION tbl_top_io_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        NULLIF(heap_blks_read, 0) as heap_blks_read,
        NULLIF(heap_blks_read_pct, 0.0) as heap_blks_read_pct,
        NULLIF(idx_blks_read, 0) as idx_blks_read,
        NULLIF(idx_blks_read_pct, 0.0) as idx_blks_read_pct,
        NULLIF(toast_blks_read, 0) as toast_blks_read,
        NULLIF(toast_blks_read_pct, 0.0) as toast_blks_read_pct,
        NULLIF(tidx_blks_read, 0) as tidx_blks_read,
        NULLIF(tidx_blks_read_pct, 0.0) as tidx_blks_read_pct,
        100.0 - (COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) +
          COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0)) * 100.0 /
        NULLIF(heap_blks_fetch + idx_blks_fetch +
          toast_blks_fetch + tidx_blks_fetch, 0) as hit_pct
    FROM top_io_tables1
    WHERE COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0) > 0
    ORDER BY
      COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0) DESC,
      datid ASC,
      relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th colspan="2">Heap</th>'
            '<th colspan="2">Ix</th>'
            '<th colspan="2">TOAST</th>'
            '<th colspan="2">TOAST-Ix</th>'
            '<th rowspan="2" title="Number of heap, indexes, toast and toast index blocks '
              'fetched from shared buffers as a percentage of all their blocks fetched from '
              'shared buffers and file system">Hit(%)</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of disk blocks read from this table">Blks</th>'
            '<th title="Heap block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from all indexes on this table">Blks</th>'
            '<th title="Indexes block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from this table''s TOAST table (if any)">Blks</th>'
            '<th title="TOAST block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from this table''s TOAST table indexes (if any)">Blks</th>'
            '<th title="TOAST table index block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_read,
            round(r_result.heap_blks_read_pct,2),
            r_result.idx_blks_read,
            round(r_result.idx_blks_read_pct,2),
            r_result.toast_blks_read,
            round(r_result.toast_blks_read_pct,2),
            r_result.tidx_blks_read,
            round(r_result.tidx_blks_read_pct,2),
            round(r_result.hit_pct,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tbl_top_io_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) AS dbname,
        COALESCE(st1.schemaname,st2.schemaname) AS schemaname,
        COALESCE(st1.relname,st2.relname) AS relname,
        NULLIF(st1.heap_blks_read, 0) AS heap_blks_read1,
        NULLIF(st1.heap_blks_read_pct, 0.0) AS heap_blks_read_pct1,
        NULLIF(st1.idx_blks_read, 0) AS idx_blks_read1,
        NULLIF(st1.idx_blks_read_pct, 0.0) AS idx_blks_read_pct1,
        NULLIF(st1.toast_blks_read, 0) AS toast_blks_read1,
        NULLIF(st1.toast_blks_read_pct, 0.0) AS toast_blks_read_pct1,
        NULLIF(st1.tidx_blks_read, 0) AS tidx_blks_read1,
        NULLIF(st1.tidx_blks_read_pct, 0.0) AS tidx_blks_read_pct1,
        100.0 - (COALESCE(st1.heap_blks_read, 0) + COALESCE(st1.idx_blks_read, 0) +
          COALESCE(st1.toast_blks_read, 0) + COALESCE(st1.tidx_blks_read, 0)) * 100.0 /
        NULLIF(st1.heap_blks_fetch + st1.idx_blks_fetch +
          st1.toast_blks_fetch + st1.tidx_blks_fetch, 0) as hit_pct1,
        NULLIF(st2.heap_blks_read, 0) AS heap_blks_read2,
        NULLIF(st2.heap_blks_read_pct, 0.0) AS heap_blks_read_pct2,
        NULLIF(st2.idx_blks_read, 0) AS idx_blks_read2,
        NULLIF(st2.idx_blks_read_pct, 0.0) AS idx_blks_read_pct2,
        NULLIF(st2.toast_blks_read, 0) AS toast_blks_read2,
        NULLIF(st2.toast_blks_read_pct, 0.0) AS toast_blks_read_pct2,
        NULLIF(st2.tidx_blks_read, 0) AS tidx_blks_read2,
        NULLIF(st2.tidx_blks_read_pct, 0.0) AS tidx_blks_read_pct2,
        100.0 - (COALESCE(st2.heap_blks_read, 0) + COALESCE(st2.idx_blks_read, 0) +
          COALESCE(st2.toast_blks_read, 0) + COALESCE(st2.tidx_blks_read, 0)) * 100.0 /
        NULLIF(st2.heap_blks_fetch + st2.idx_blks_fetch +
          st2.toast_blks_fetch + st2.tidx_blks_fetch, 0) as hit_pct2,
        row_number() OVER (ORDER BY COALESCE(st1.heap_blks_read, 0) + COALESCE(st1.idx_blks_read, 0) +
          COALESCE(st1.toast_blks_read, 0) + COALESCE(st1.tidx_blks_read, 0) DESC NULLS LAST) rn_read1,
        row_number() OVER (ORDER BY COALESCE(st2.heap_blks_read, 0) + COALESCE(st2.idx_blks_read, 0) +
          COALESCE(st2.toast_blks_read, 0) + COALESCE(st2.tidx_blks_read, 0) DESC NULLS LAST) rn_read2
    FROM top_io_tables1 st1
        FULL OUTER JOIN top_io_tables2 st2 USING (server_id, datid, relid)
    WHERE COALESCE(st1.heap_blks_read, 0) + COALESCE(st1.idx_blks_read, 0) +
          COALESCE(st1.toast_blks_read, 0) + COALESCE(st1.tidx_blks_read, 0) +
          COALESCE(st2.heap_blks_read, 0) + COALESCE(st2.idx_blks_read, 0) +
          COALESCE(st2.toast_blks_read, 0) + COALESCE(st2.tidx_blks_read, 0) > 0
    ORDER BY
      COALESCE(st1.heap_blks_read, 0) + COALESCE(st1.idx_blks_read, 0) +
      COALESCE(st1.toast_blks_read, 0) + COALESCE(st1.tidx_blks_read, 0) +
      COALESCE(st2.heap_blks_read, 0) + COALESCE(st2.idx_blks_read, 0) +
      COALESCE(st2.toast_blks_read, 0) + COALESCE(st2.tidx_blks_read, 0) DESC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.relid,st2.relid) ASC
    ) t1
    WHERE least(
        rn_read1,
        rn_read2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="2">Heap</th>'
            '<th colspan="2">Ix</th>'
            '<th colspan="2">TOAST</th>'
            '<th colspan="2">TOAST-Ix</th>'
            '<th rowspan="2" title="Number of heap, indexes, toast and toast index blocks '
              'fetched from shared buffers as a percentage of all their blocks fetched from '
              'shared buffers and file system">Hit(%)</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of disk blocks read from this table">Blks</th>'
            '<th title="Heap block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from all indexes on this table">Blks</th>'
            '<th title="Indexes block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from this table''s TOAST table (if any)">Blks</th>'
            '<th title="TOAST block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Number of disk blocks read from this table''s TOAST table indexes (if any)">Blks</th>'
            '<th title="TOAST table index block reads for this table as a percentage of all blocks read in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_read1,
            round(r_result.heap_blks_read_pct1,2),
            r_result.idx_blks_read1,
            round(r_result.idx_blks_read_pct1,2),
            r_result.toast_blks_read1,
            round(r_result.toast_blks_read_pct1,2),
            r_result.tidx_blks_read1,
            round(r_result.tidx_blks_read_pct1,2),
            round(r_result.hit_pct1,2),
            r_result.heap_blks_read2,
            round(r_result.heap_blks_read_pct2,2),
            r_result.idx_blks_read2,
            round(r_result.idx_blks_read_pct2,2),
            r_result.toast_blks_read2,
            round(r_result.toast_blks_read_pct2,2),
            r_result.tidx_blks_read2,
            round(r_result.tidx_blks_read_pct2,2),
            round(r_result.hit_pct2,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tbl_top_fetch_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        NULLIF(heap_blks_fetch, 0) as heap_blks_fetch,
        NULLIF(heap_blks_proc_pct, 0.0) as heap_blks_proc_pct,
        NULLIF(idx_blks_fetch, 0) as idx_blks_fetch,
        NULLIF(idx_blks_fetch_pct, 0.0) as idx_blks_fetch_pct,
        NULLIF(toast_blks_fetch, 0) as toast_blks_fetch,
        NULLIF(toast_blks_fetch_pct, 0.0) as toast_blks_fetch_pct,
        NULLIF(tidx_blks_fetch, 0) as tidx_blks_fetch,
        NULLIF(tidx_blks_fetch_pct, 0.0) as tidx_blks_fetch_pct
    FROM top_io_tables1
    WHERE COALESCE(heap_blks_fetch, 0) + COALESCE(idx_blks_fetch, 0) + COALESCE(toast_blks_fetch, 0) + COALESCE(tidx_blks_fetch, 0) > 0
    ORDER BY
      COALESCE(heap_blks_fetch, 0) + COALESCE(idx_blks_fetch, 0) + COALESCE(toast_blks_fetch, 0) + COALESCE(tidx_blks_fetch, 0) DESC,
      datid ASC,
      relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th colspan="2">Heap</th>'
            '<th colspan="2">Ix</th>'
            '<th colspan="2">TOAST</th>'
            '<th colspan="2">TOAST-Ix</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of blocks fetched (read+hit) from this table">Blks</th>'
            '<th title="Heap blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from all indexes on this table">Blks</th>'
            '<th title="Indexes blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from this table''s TOAST table (if any)">Blks</th>'
            '<th title="TOAST blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from this table''s TOAST table indexes (if any)">Blks</th>'
            '<th title="TOAST table index blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_fetch,
            round(r_result.heap_blks_proc_pct,2),
            r_result.idx_blks_fetch,
            round(r_result.idx_blks_fetch_pct,2),
            r_result.toast_blks_fetch,
            round(r_result.toast_blks_fetch_pct,2),
            r_result.tidx_blks_fetch,
            round(r_result.tidx_blks_fetch_pct,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tbl_top_fetch_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) AS dbname,
        COALESCE(st1.tablespacename,st2.tablespacename) AS tablespacename,
        COALESCE(st1.schemaname,st2.schemaname) AS schemaname,
        COALESCE(st1.relname,st2.relname) AS relname,
        NULLIF(st1.heap_blks_fetch, 0) AS heap_blks_fetch1,
        NULLIF(st1.heap_blks_proc_pct, 0.0) AS heap_blks_proc_pct1,
        NULLIF(st1.idx_blks_fetch, 0) AS idx_blks_fetch1,
        NULLIF(st1.idx_blks_fetch_pct, 0.0) AS idx_blks_fetch_pct1,
        NULLIF(st1.toast_blks_fetch, 0) AS toast_blks_fetch1,
        NULLIF(st1.toast_blks_fetch_pct, 0.0) AS toast_blks_fetch_pct1,
        NULLIF(st1.tidx_blks_fetch, 0) AS tidx_blks_fetch1,
        NULLIF(st1.tidx_blks_fetch_pct, 0.0) AS tidx_blks_fetch_pct1,
        NULLIF(st2.heap_blks_fetch, 0) AS heap_blks_fetch2,
        NULLIF(st2.heap_blks_proc_pct, 0.0) AS heap_blks_proc_pct2,
        NULLIF(st2.idx_blks_fetch, 0) AS idx_blks_fetch2,
        NULLIF(st2.idx_blks_fetch_pct, 0.0) AS idx_blks_fetch_pct2,
        NULLIF(st2.toast_blks_fetch, 0) AS toast_blks_fetch2,
        NULLIF(st2.toast_blks_fetch_pct, 0.0) AS toast_blks_fetch_pct2,
        NULLIF(st2.tidx_blks_fetch, 0) AS tidx_blks_fetch2,
        NULLIF(st2.tidx_blks_fetch_pct, 0.0) AS tidx_blks_fetch_pct2,
        row_number() OVER (ORDER BY COALESCE(st1.heap_blks_fetch, 0) + COALESCE(st1.idx_blks_fetch, 0) +
          COALESCE(st1.toast_blks_fetch, 0) + COALESCE(st1.tidx_blks_fetch, 0) DESC NULLS LAST) rn_fetched1,
        row_number() OVER (ORDER BY COALESCE(st2.heap_blks_fetch, 0) + COALESCE(st2.idx_blks_fetch, 0) +
          COALESCE(st2.toast_blks_fetch, 0) + COALESCE(st2.tidx_blks_fetch, 0) DESC NULLS LAST) rn_fetched2
    FROM top_io_tables1 st1
        FULL OUTER JOIN top_io_tables2 st2 USING (server_id, datid, relid)
    WHERE COALESCE(st1.heap_blks_fetch, 0) + COALESCE(st1.idx_blks_fetch, 0) + COALESCE(st1.toast_blks_fetch, 0) + COALESCE(st1.tidx_blks_fetch, 0) +
        COALESCE(st2.heap_blks_fetch, 0) + COALESCE(st2.idx_blks_fetch, 0) + COALESCE(st2.toast_blks_fetch, 0) + COALESCE(st2.tidx_blks_fetch, 0) > 0
    ORDER BY
      COALESCE(st1.heap_blks_fetch, 0) + COALESCE(st1.idx_blks_fetch, 0) + COALESCE(st1.toast_blks_fetch, 0) + COALESCE(st1.tidx_blks_fetch, 0) +
      COALESCE(st2.heap_blks_fetch, 0) + COALESCE(st2.idx_blks_fetch, 0) + COALESCE(st2.toast_blks_fetch, 0) + COALESCE(st2.tidx_blks_fetch, 0) DESC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.relid,st2.relid) ASC
    ) t1
    WHERE least(
        rn_fetched1,
        rn_fetched2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="2">Heap</th>'
            '<th colspan="2">Ix</th>'
            '<th colspan="2">TOAST</th>'
            '<th colspan="2">TOAST-Ix</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of blocks fetched (read+hit) from this table">Blks</th>'
            '<th title="Heap blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from all indexes on this table">Blks</th>'
            '<th title="Indexes blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from this table''s TOAST table (if any)">Blks</th>'
            '<th title="TOAST blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
            '<th title="Number of blocks fetched (read+hit) from this table''s TOAST table indexes (if any)">Blks</th>'
            '<th title="TOAST table index blocks fetched for this table as a percentage of all blocks fetched in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_fetch1,
            round(r_result.heap_blks_proc_pct1,2),
            r_result.idx_blks_fetch1,
            round(r_result.idx_blks_fetch_pct1,2),
            r_result.toast_blks_fetch1,
            round(r_result.toast_blks_fetch_pct1,2),
            r_result.tidx_blks_fetch1,
            round(r_result.tidx_blks_fetch_pct1,2),
            r_result.heap_blks_fetch2,
            round(r_result.heap_blks_proc_pct2,2),
            r_result.idx_blks_fetch2,
            round(r_result.idx_blks_fetch_pct2,2),
            r_result.toast_blks_fetch2,
            round(r_result.toast_blks_fetch_pct2,2),
            r_result.tidx_blks_fetch2,
            round(r_result.tidx_blks_fetch_pct2,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_top_io_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        indexrelname,
        NULLIF(idx_scan, 0) as idx_scan,
        NULLIF(idx_blks_read, 0) as idx_blks_read,
        NULLIF(idx_blks_read_pct, 0.0) as idx_blks_read_pct,
        NULLIF(idx_blks_hit_pct, 0.0) as idx_blks_hit_pct
    FROM top_io_indexes1
    WHERE idx_blks_read > 0
    ORDER BY
      idx_blks_read DESC,
      datid ASC,
      relid ASC,
      indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th title="Number of scans performed on index">Scans</th>'
            '<th title="Number of disk blocks read from this index">Blk Reads</th>'
            '<th title="Disk blocks read from this index as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Index blocks buffer cache hit percentage">Hits(%)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_scan,
        r_result.idx_blks_read,
        round(r_result.idx_blks_read_pct,2),
        round(r_result.idx_blks_hit_pct,2)
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_top_io_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.tablespacename,st2.tablespacename) as tablespacename,
        COALESCE(st1.schemaname,st2.schemaname) as schemaname,
        COALESCE(st1.relname,st2.relname) as relname,
        COALESCE(st1.indexrelname,st2.indexrelname) as indexrelname,
        NULLIF(st1.idx_scan, 0) as idx_scan1,
        NULLIF(st1.idx_blks_read, 0) as idx_blks_read1,
        NULLIF(st1.idx_blks_read_pct, 0.0) as idx_blks_read_pct1,
        NULLIF(st1.idx_blks_hit_pct, 0.0) as idx_blks_hit_pct1,
        NULLIF(st2.idx_scan, 0) as idx_scan2,
        NULLIF(st2.idx_blks_read, 0) as idx_blks_read2,
        NULLIF(st2.idx_blks_read_pct, 0.0) as idx_blks_read_pct2,
        NULLIF(st2.idx_blks_hit_pct, 0.0) as idx_blks_hit_pct2,
        row_number() OVER (ORDER BY st1.idx_blks_read DESC NULLS LAST) as rn_read1,
        row_number() OVER (ORDER BY st2.idx_blks_read DESC NULLS LAST) as rn_read2
    FROM
        top_io_indexes1 st1
        FULL OUTER JOIN top_io_indexes2 st2 USING (server_id, datid, relid, indexrelid)
    WHERE COALESCE(st1.idx_blks_read, 0) + COALESCE(st2.idx_blks_read, 0) > 0
    ORDER BY
      COALESCE(st1.idx_blks_read, 0) + COALESCE(st2.idx_blks_read, 0) DESC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.relid,st2.relid) ASC,
      COALESCE(st1.indexrelid,st2.indexrelid) ASC
    ) t1
    WHERE least(
        rn_read1,
        rn_read2
      ) <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th>I</th>'
            '<th title="Number of scans performed on index">Scans</th>'
            '<th title="Number of disk blocks read from this index">Blk Reads</th>'
            '<th title="Disk blocks read from this index as a percentage of all blocks read in a cluster">%Total</th>'
            '<th title="Index blocks buffer cache hit percentage">Hits(%)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_scan1,
        r_result.idx_blks_read1,
        round(r_result.idx_blks_read_pct1,2),
        round(r_result.idx_blks_hit_pct1,2),
        r_result.idx_scan2,
        r_result.idx_blks_read2,
        round(r_result.idx_blks_read_pct2,2),
        round(r_result.idx_blks_hit_pct2,2)
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_top_fetch_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        indexrelname,
        NULLIF(idx_scan, 0) as idx_scan,
        NULLIF(idx_blks_fetch, 0) as idx_blks_fetch,
        NULLIF(idx_blks_fetch_pct, 0.0) as idx_blks_fetch_pct
    FROM top_io_indexes1
    WHERE idx_blks_fetch > 0
    ORDER BY
      idx_blks_fetch DESC,
      datid ASC,
      relid ASC,
      indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th title="Number of scans performed on index">Scans</th>'
            '<th title="Number of blocks fetched (read+hit) from this index">Blks</th>'
            '<th title="Blocks fetched from this index as a percentage of all blocks fetched in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_scan,
        r_result.idx_blks_fetch,
        round(r_result.idx_blks_fetch_pct,2)
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_top_fetch_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.tablespacename,st2.tablespacename) as tablespacename,
        COALESCE(st1.schemaname,st2.schemaname) as schemaname,
        COALESCE(st1.relname,st2.relname) as relname,
        COALESCE(st1.indexrelname,st2.indexrelname) as indexrelname,
        NULLIF(st1.idx_scan, 0) as idx_scan1,
        NULLIF(st1.idx_blks_fetch, 0) as idx_blks_fetch1,
        NULLIF(st1.idx_blks_fetch_pct, 0.0) as idx_blks_fetch_pct1,
        NULLIF(st2.idx_scan, 0) as idx_scan2,
        NULLIF(st2.idx_blks_fetch, 0) as idx_blks_fetch2,
        NULLIF(st2.idx_blks_fetch_pct, 0.0) as idx_blks_fetch_pct2,
        row_number() OVER (ORDER BY st1.idx_blks_fetch DESC NULLS LAST) as rn_fetched1,
        row_number() OVER (ORDER BY st2.idx_blks_fetch DESC NULLS LAST) as rn_fetched2
    FROM
        top_io_indexes1 st1
        FULL OUTER JOIN top_io_indexes2 st2 USING (server_id, datid, relid, indexrelid)
    WHERE COALESCE(st1.idx_blks_fetch, 0) + COALESCE(st2.idx_blks_fetch, 0) > 0
    ORDER BY
      COALESCE(st1.idx_blks_fetch, 0) + COALESCE(st2.idx_blks_fetch, 0) DESC,
      COALESCE(st1.datid,st2.datid) ASC,
      COALESCE(st1.relid,st2.relid) ASC,
      COALESCE(st1.indexrelid,st2.indexrelid) ASC
    ) t1
    WHERE least(
        rn_fetched1,
        rn_fetched2
      ) <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th>I</th>'
            '<th title="Number of scans performed on index">Scans</th>'
            '<th title="Number of blocks fetched (read+hit) from this index">Blks</th>'
            '<th title="Blocks fetched from this index as a percentage of all blocks fetched in a cluster">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates

    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_scan1,
        r_result.idx_blks_fetch1,
        round(r_result.idx_blks_fetch_pct1,2),
        r_result.idx_scan2,
        r_result.idx_blks_fetch2,
        round(r_result.idx_blks_fetch_pct2,2)
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
/* ===== Cluster stats functions ===== */
CREATE FUNCTION profile_checkavail_walstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there is table sizes collected in both bounds
  SELECT
    count(wal_bytes) > 0
  FROM sample_stat_wal
  WHERE
    server_id = sserver_id
    AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        server_id               integer,
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        wal_buffers_full    bigint,
        wal_write           bigint,
        wal_sync            bigint,
        wal_write_time      double precision,
        wal_sync_time       double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id as server_id,
        sum(wal_records)::bigint as wal_records,
        sum(wal_fpi)::bigint as wal_fpi,
        sum(wal_bytes)::numeric as wal_bytes,
        sum(wal_buffers_full)::bigint as wal_buffers_full,
        sum(wal_write)::bigint as wal_write,
        sum(wal_sync)::bigint as wal_sync,
        sum(wal_write_time)::double precision as wal_write_time,
        sum(wal_sync_time)::double precision as wal_sync_time
    FROM sample_stat_wal st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        sample_id        integer,
        wal_stats_reset  timestamp with time zone
)
SET search_path=@extschema@ AS $$
  SELECT
      ws1.sample_id as sample_id,
      nullif(ws1.stats_reset,ws0.stats_reset)
  FROM sample_stat_wal ws1
      JOIN sample_stat_wal ws0 ON (ws1.server_id = ws0.server_id AND ws1.sample_id = ws0.sample_id + 1)
  WHERE ws1.server_id = sserver_id AND ws1.sample_id BETWEEN start_id + 1 AND end_id
    AND
      nullif(ws1.stats_reset,ws0.stats_reset) IS NOT NULL
  ORDER BY ws1.sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_reset_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer) FOR
    SELECT
        sample_id,
        wal_stats_reset
    FROM wal_stats_reset(sserver_id,start1_id,end1_id)
    ORDER BY wal_stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Sample</th>'
            '<th>WAL stats reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl',
        '<tr>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['sample_tpl'],
            r_result.sample_id,
            r_result.wal_stats_reset
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION wal_stats_reset_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer) FOR
    SELECT
        interval_num,
        sample_id,
        wal_stats_reset
    FROM
      (SELECT 1 AS interval_num, sample_id, wal_stats_reset
        FROM wal_stats_reset(sserver_id,start1_id,end1_id)
      UNION ALL
      SELECT 2 AS interval_num, sample_id, wal_stats_reset
        FROM wal_stats_reset(sserver_id,start2_id,end2_id)) AS samples
    ORDER BY interval_num, wal_stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>I</th>'
            '<th>Sample</th>'
            '<th>WAL stats reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl1',
        '<tr {interval1}>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'sample_tpl2',
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
      CASE r_result.interval_num
        WHEN 1 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl1'],
              r_result.sample_id,
              r_result.wal_stats_reset
          );
        WHEN 2 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl2'],
              r_result.sample_id,
              r_result.wal_stats_reset
          );
        END CASE;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION wal_stats_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    report_duration float = (report_context #> ARRAY['report_properties','interval_duration_sec'])::float;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer) FOR
    SELECT
        NULLIF(wal_records, 0) as wal_records,
        NULLIF(wal_fpi, 0) as wal_fpi,
        NULLIF(wal_bytes, 0) as wal_bytes,
        NULLIF(wal_buffers_full, 0) as wal_buffers_full,
        NULLIF(wal_write, 0) as wal_write,
        NULLIF(wal_sync, 0) as wal_sync,
        NULLIF(wal_write_time, 0.0) as wal_write_time,
        NULLIF(wal_sync_time, 0.0) as wal_sync_time
    FROM wal_stats(sserver_id,start1_id,end1_id);

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Metric</th>'
            '<th>Value</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'val_tpl',
        '<tr>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer
      )
    LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of WAL generated', 'WAL generated', pg_size_pretty(r_result.wal_bytes));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average amount of WAL generated per second', 'WAL per second',
          pg_size_pretty(
            round(
              r_result.wal_bytes/report_duration
            )::bigint
          ));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total number of WAL records generated', 'WAL records', r_result.wal_records);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total number of WAL full page images generated', 'WAL FPI', r_result.wal_fpi);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL data was written to disk because WAL buffers became full',
          'WAL buffers full', r_result.wal_buffers_full);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL buffers were written out to disk via XLogWrite request',
          'WAL writes', r_result.wal_write);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average number of times WAL buffers were written out to disk via XLogWrite request per second',
          'WAL writes per second',
          round((r_result.wal_write/report_duration)::numeric,2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL files were synced to disk via issue_xlog_fsync request (if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)',
          'WAL sync', r_result.wal_sync);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average number of times WAL files were synced to disk via issue_xlog_fsync request per second',
          'WAL syncs per second',
          round((r_result.wal_sync/report_duration)::numeric,2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of time spent writing WAL buffers to disk via XLogWrite request, in milliseconds (if track_wal_io_timing is enabled, otherwise zero). This includes the sync time when wal_sync_method is either open_datasync or open_sync',
          'WAL write time (s)',
          round(cast(r_result.wal_write_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'WAL write time as a percentage of the report duration time',
          'WAL write duty',
          round((r_result.wal_write_time/10/report_duration)::numeric,2) || '%');
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of time spent syncing WAL files to disk via issue_xlog_fsync request, in milliseconds (if track_wal_io_timing is enabled, fsync is on, and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)',
          'WAL sync time (s)',
          round(cast(r_result.wal_sync_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'WAL sync time as a percentage of the report duration time',
          'WAL sync duty',
          round((r_result.wal_sync_time/10/report_duration)::numeric,2) || '%');
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION wal_stats_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    report1_duration float = (report_context #> ARRAY['report_properties','interval1_duration_sec'])::float;
    report2_duration float = (report_context #> ARRAY['report_properties','interval2_duration_sec'])::float;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer) FOR
    SELECT
        NULLIF(stat1.wal_records, 0) as wal_records1,
        NULLIF(stat1.wal_fpi, 0) as wal_fpi1,
        NULLIF(stat1.wal_bytes, 0) as wal_bytes1,
        NULLIF(stat1.wal_buffers_full, 0) as wal_buffers_full1,
        NULLIF(stat1.wal_write, 0) as wal_write1,
        NULLIF(stat1.wal_sync, 0) as wal_sync1,
        NULLIF(stat1.wal_write_time, 0.0) as wal_write_time1,
        NULLIF(stat1.wal_sync_time, 0.0) as wal_sync_time1,
        NULLIF(stat2.wal_records, 0) as wal_records2,
        NULLIF(stat2.wal_fpi, 0) as wal_fpi2,
        NULLIF(stat2.wal_bytes, 0) as wal_bytes2,
        NULLIF(stat2.wal_buffers_full, 0) as wal_buffers_full2,
        NULLIF(stat2.wal_write, 0) as wal_write2,
        NULLIF(stat2.wal_sync, 0) as wal_sync2,
        NULLIF(stat2.wal_write_time, 0.0) as wal_write_time2,
        NULLIF(stat2.wal_sync_time, 0.0) as wal_sync_time2
    FROM wal_stats(sserver_id,start1_id,end1_id) stat1
        FULL OUTER JOIN wal_stats(sserver_id,start2_id,end2_id) stat2 USING (server_id);

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Metric</th>'
            '<th {title1}>Value (1)</th>'
            '<th {title2}>Value (2)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'val_tpl',
        '<tr>'
          '<td title="%s">%s</td>'
          '<td {interval1}><div {value}>%s</div></td>'
          '<td {interval2}><div {value}>%s</div></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of WAL generated', 'WAL generated',
          pg_size_pretty(r_result.wal_bytes1), pg_size_pretty(r_result.wal_bytes2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average amount of WAL generated per second', 'WAL per second',
          pg_size_pretty(
            round(
              r_result.wal_bytes1/report1_duration
            )::bigint
          ),
          pg_size_pretty(
            round(
              r_result.wal_bytes2/report2_duration
            )::bigint
          ));

        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total number of WAL records generated', 'WAL records', r_result.wal_records1, r_result.wal_records2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total number of WAL full page images generated', 'WAL FPI', r_result.wal_fpi1, r_result.wal_fpi2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL data was written to disk because WAL buffers became full', 'WAL buffers full', r_result.wal_buffers_full1, r_result.wal_buffers_full2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL buffers were written out to disk via XLogWrite request', 'WAL writes',
          r_result.wal_write1, r_result.wal_write2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average number of times WAL buffers were written out to disk via XLogWrite request per second',
          'WAL writes per second',
          round((r_result.wal_write1/report1_duration)::numeric,2),
          round((r_result.wal_write2/report2_duration)::numeric,2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL files were synced to disk via issue_xlog_fsync request (if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)',
          'WAL sync', r_result.wal_sync1, r_result.wal_sync2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average number of times WAL files were synced to disk via issue_xlog_fsync request per second',
          'WAL syncs per second',
          round((r_result.wal_sync1/report1_duration)::numeric,2),
          round((r_result.wal_sync2/report2_duration)::numeric,2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of time spent writing WAL buffers to disk via XLogWrite request, in milliseconds (if track_wal_io_timing is enabled, otherwise zero). This includes the sync time when wal_sync_method is either open_datasync or open_sync',
          'WAL write time (s)',
          round(cast(r_result.wal_write_time1/1000 as numeric),2),
          round(cast(r_result.wal_write_time2/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'WAL write time as a percentage of the report duration time',
          'WAL write duty',
          round((r_result.wal_write_time1/10/report1_duration)::numeric,2) || '%',
          round((r_result.wal_write_time2/10/report2_duration)::numeric,2) || '%');
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of time spent syncing WAL files to disk via issue_xlog_fsync request, in milliseconds (if track_wal_io_timing is enabled, fsync is on, and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)',
          'WAL sync time (s)',
          round(cast(r_result.wal_sync_time1/1000 as numeric),2),
          round(cast(r_result.wal_sync_time2/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'WAL sync time as a percentage of the report duration time',
          'WAL sync duty',
          round((r_result.wal_sync_time1/10/report1_duration)::numeric,2) || '%',
          round((r_result.wal_sync_time2/10/report2_duration)::numeric,2) || '%');
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
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

CREATE FUNCTION get_report_template(IN report_context jsonb, IN report_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
  tpl         text = NULL;

  c_tpl_sbst  CURSOR (template text, type text) FOR
  SELECT DISTINCT s[1] AS type, s[2] AS item
  FROM regexp_matches(template, '{('||type||'):'||$o$(\w+)}$o$,'g') AS s;

  r_result    RECORD;
BEGIN
  SELECT static_text INTO STRICT tpl
  FROM report r JOIN report_static rs ON (rs.static_name = r.template)
  WHERE r.report_id = get_report_template.report_id;

  ASSERT tpl IS NOT NULL, 'Report template not found';
  -- Static content first
  -- Not found static placeholders silently removed
  WHILE strpos(tpl, '{static:') > 0 LOOP
    FOR r_result IN c_tpl_sbst(tpl, 'static') LOOP
      IF r_result.type = 'static' THEN
        tpl := replace(tpl, format('{%s:%s}', r_result.type, r_result.item),
          COALESCE((SELECT static_text FROM report_static WHERE static_name = r_result.item), '')
        );
      END IF;
    END LOOP; -- over static substitutions
  END LOOP; -- over static placeholders

  -- Properties substitution next
  WHILE strpos(tpl, '{properties:') > 0 LOOP
    FOR r_result IN c_tpl_sbst(tpl, 'properties') LOOP
      IF r_result.type = 'properties' THEN
        ASSERT report_context #>> ARRAY['report_properties', r_result.item] IS NOT NULL,
          'Property % not found',
          format('{%s,$%s}', r_result.type, r_result.item);
        tpl := replace(tpl, format('{%s:%s}', r_result.type, r_result.item),
          report_context #>> ARRAY['report_properties', r_result.item]
        );
      END IF;
    END LOOP; -- over properties substitutions
  END LOOP; -- over properties placeholders
  ASSERT tpl IS NOT NULL, 'Report template lost during substitution';

  RETURN tpl;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION init_report_temp_tables(IN report_context jsonb, IN sserver_id integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  start1_id   integer = (report_context #>> '{report_properties,start1_id}')::integer;
  start2_id   integer = (report_context #>> '{report_properties,start2_id}')::integer;
  end1_id     integer = (report_context #>> '{report_properties,end1_id}')::integer;
  end2_id     integer = (report_context #>> '{report_properties,end2_id}')::integer;
BEGIN
    -- Report internal temporary tables
    -- Creating temporary table for reported queries
    CREATE TEMPORARY TABLE IF NOT EXISTS queries_list (
      userid              oid,
      datid               oid,
      queryid             bigint,
      CONSTRAINT pk_queries_list PRIMARY KEY (userid, datid, queryid))
    ON COMMIT DELETE ROWS;

    /*
    * Caching temporary tables, containing object stats cache
    * used several times in a report functions
    */
    CREATE TEMPORARY TABLE top_statements1 AS
    SELECT * FROM top_statements(sserver_id, start1_id, end1_id);

    /* table size is collected in a sample when relsize field is not null
    In a report we can use relsize-based growth calculated as a sum of
    relsize increments only when sizes was collected
    in the both first and last sample, otherwise we only can use
    pg_class.relpages
    */
    CREATE TEMPORARY TABLE top_tables1 AS
    SELECT tt.*,
      rs.relsize_growth_avail AS relsize_growth_avail,
      CASE WHEN rs.relsize_growth_avail THEN
        tt.growth
      ELSE
        tt.relpagegrowth_bytes
      END AS best_growth,
      rs.relsize_toastgrowth_avail AS relsize_toastgrowth_avail,
      CASE WHEN rs.relsize_toastgrowth_avail THEN
        tt.toastgrowth
      ELSE
        tt.toastrelpagegrowth_bytes
      END AS best_toastgrowth,
      CASE WHEN tt.seqscan_relsize_avail THEN
        tt.seqscan_bytes_relsize
      ELSE
        tt.seqscan_bytes_relpages
      END AS best_seqscan_bytes,
      CASE WHEN tt.t_seqscan_relsize_avail THEN
        tt.t_seqscan_bytes_relsize
      ELSE
        tt.t_seqscan_bytes_relpages
      END AS best_t_seqscan_bytes
    FROM top_tables(sserver_id, start1_id, end1_id) tt
    JOIN (
      SELECT rel.server_id, rel.datid, rel.relid,
          COALESCE(
              max(rel.sample_id) = max(rel.sample_id) FILTER (WHERE rel.relsize IS NOT NULL)
              AND min(rel.sample_id) = min(rel.sample_id) FILTER (WHERE rel.relsize IS NOT NULL)
          , false) AS relsize_growth_avail,
          COALESCE(
              max(reltoast.sample_id) = max(reltoast.sample_id) FILTER (WHERE reltoast.relsize IS NOT NULL)
              AND min(reltoast.sample_id) = min(reltoast.sample_id) FILTER (WHERE reltoast.relsize IS NOT NULL)
          , false) AS relsize_toastgrowth_avail
      FROM sample_stat_tables rel
          JOIN tables_list tl USING (server_id, datid, relid)
          LEFT JOIN sample_stat_tables reltoast ON
              (rel.server_id, rel.sample_id, rel.datid, tl.reltoastrelid) =
              (reltoast.server_id, reltoast.sample_id, reltoast.datid, reltoast.relid)
      WHERE
          rel.server_id = sserver_id
          AND rel.sample_id BETWEEN start1_id AND end1_id
      GROUP BY rel.server_id, rel.datid, rel.relid
    ) rs USING (server_id, datid, relid);

    CREATE TEMPORARY TABLE top_indexes1 AS
    SELECT ti.*,
      rs.relsize_growth_avail AS relsize_growth_avail,
      CASE WHEN rs.relsize_growth_avail THEN
        ti.growth
      ELSE
        ti.relpagegrowth_bytes
      END AS best_growth
    FROM top_indexes(sserver_id, start1_id, end1_id) ti
    JOIN (
      SELECT server_id, datid, indexrelid,
          COALESCE(
              max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL)
              AND min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL)
          , false) AS relsize_growth_avail
      FROM sample_stat_indexes
      WHERE
          server_id = sserver_id
          AND sample_id BETWEEN start1_id AND end1_id
      GROUP BY server_id, datid, indexrelid
    ) rs USING (server_id, datid, indexrelid);

    CREATE TEMPORARY TABLE top_io_tables1 AS
    SELECT * FROM top_io_tables(sserver_id, start1_id, end1_id);
    CREATE TEMPORARY TABLE top_io_indexes1 AS
    SELECT * FROM top_io_indexes(sserver_id, start1_id, end1_id);
    CREATE TEMPORARY TABLE top_functions1 AS
    SELECT * FROM top_functions(sserver_id, start1_id, end1_id, false);
    CREATE TEMPORARY TABLE top_kcache_statements1 AS
    SELECT * FROM top_kcache_statements(sserver_id, start1_id, end1_id);

    ANALYZE top_statements1;
    ANALYZE top_tables1;
    ANALYZE top_indexes1;
    ANALYZE top_io_tables1;
    ANALYZE top_io_indexes1;
    ANALYZE top_functions1;
    ANALYZE top_kcache_statements1;

    IF jsonb_extract_path_text(report_context, 'report_features', 'wait_sampling_tot')::boolean THEN
      CREATE TEMPORARY TABLE wait_sampling_total_stats1 AS
      SELECT * FROM wait_sampling_total_stats(sserver_id, start1_id, end1_id);
      ANALYZE wait_sampling_total_stats1;
    END IF;

    IF num_nulls(start2_id, end2_id) = 0 THEN
      CREATE TEMPORARY TABLE top_statements2 AS
      SELECT * FROM top_statements(sserver_id, start2_id, end2_id);

      CREATE TEMPORARY TABLE top_tables2 AS
      SELECT tt.*,
        rs.relsize_growth_avail AS relsize_growth_avail,
        CASE WHEN rs.relsize_growth_avail THEN
          tt.growth
        ELSE
          tt.relpagegrowth_bytes
        END AS best_growth,
        rs.relsize_toastgrowth_avail AS relsize_toastgrowth_avail,
        CASE WHEN rs.relsize_toastgrowth_avail THEN
          tt.toastgrowth
        ELSE
          tt.toastrelpagegrowth_bytes
        END AS best_toastgrowth,
        CASE WHEN tt.seqscan_relsize_avail THEN
          tt.seqscan_bytes_relsize
        ELSE
          tt.seqscan_bytes_relpages
        END AS best_seqscan_bytes,
        CASE WHEN tt.t_seqscan_relsize_avail THEN
          tt.t_seqscan_bytes_relsize
        ELSE
          tt.t_seqscan_bytes_relpages
        END AS best_t_seqscan_bytes
      FROM top_tables(sserver_id, start2_id, end2_id) tt
      JOIN (
        SELECT rel.server_id, rel.datid, rel.relid,
            COALESCE(
                max(rel.sample_id) = max(rel.sample_id) FILTER (WHERE rel.relsize IS NOT NULL)
                AND min(rel.sample_id) = min(rel.sample_id) FILTER (WHERE rel.relsize IS NOT NULL)
            , false) AS relsize_growth_avail,
            COALESCE(
                max(reltoast.sample_id) = max(reltoast.sample_id) FILTER (WHERE reltoast.relsize IS NOT NULL)
                AND min(reltoast.sample_id) = min(reltoast.sample_id) FILTER (WHERE reltoast.relsize IS NOT NULL)
            , false) AS relsize_toastgrowth_avail
        FROM sample_stat_tables rel
            JOIN tables_list tl USING (server_id, datid, relid)
            LEFT JOIN sample_stat_tables reltoast ON
                (rel.server_id, rel.sample_id, rel.datid, tl.reltoastrelid) =
                (reltoast.server_id, reltoast.sample_id, reltoast.datid, reltoast.relid)
        WHERE
            rel.server_id = sserver_id
            AND rel.sample_id BETWEEN start2_id AND end2_id
        GROUP BY rel.server_id, rel.datid, rel.relid
      ) rs USING (server_id, datid, relid);

      CREATE TEMPORARY TABLE top_indexes2 AS
      SELECT ti.*,
        rs.relsize_growth_avail AS relsize_growth_avail,
        CASE WHEN rs.relsize_growth_avail THEN
          ti.growth
        ELSE
          ti.relpagegrowth_bytes
        END AS best_growth
      FROM top_indexes(sserver_id, start2_id, end2_id) ti
      JOIN (
        SELECT server_id, datid, indexrelid,
            COALESCE(
                max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL)
                AND min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL)
            , false) AS relsize_growth_avail
        FROM sample_stat_indexes
        WHERE
            server_id = sserver_id
            AND sample_id BETWEEN start2_id AND end2_id
        GROUP BY server_id, datid, indexrelid
      ) rs USING (server_id, datid, indexrelid);

      CREATE TEMPORARY TABLE top_io_tables2 AS
      SELECT * FROM top_io_tables(sserver_id, start2_id, end2_id);
      CREATE TEMPORARY TABLE top_io_indexes2 AS
      SELECT * FROM top_io_indexes(sserver_id, start2_id, end2_id);
      CREATE TEMPORARY TABLE top_functions2 AS
      SELECT * FROM top_functions(sserver_id, start2_id, end2_id, false);
      CREATE TEMPORARY TABLE top_kcache_statements2 AS
      SELECT * FROM top_kcache_statements(sserver_id, start2_id, end2_id);

      ANALYZE top_statements2;
      ANALYZE top_tables2;
      ANALYZE top_indexes2;
      ANALYZE top_io_tables2;
      ANALYZE top_io_indexes2;
      ANALYZE top_functions2;
      ANALYZE top_kcache_statements2;
      IF jsonb_extract_path_text(report_context, 'report_features', 'wait_sampling_tot')::boolean THEN
        CREATE TEMPORARY TABLE wait_sampling_total_stats2 AS
        SELECT * FROM wait_sampling_total_stats(sserver_id, start2_id, end2_id);
        ANALYZE wait_sampling_total_stats2;
      END IF;

    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION cleanup_report_temp_tables(IN report_context jsonb, IN sserver_id integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  start2_id   integer = (report_context #>> '{report_properties,start2_id}')::integer;
  end2_id     integer = (report_context #>> '{report_properties,end2_id}')::integer;
BEGIN
  DROP TABLE top_statements1;
  DROP TABLE top_tables1;
  DROP TABLE top_indexes1;
  DROP TABLE top_io_tables1;
  DROP TABLE top_io_indexes1;
  DROP TABLE top_functions1;
  DROP TABLE top_kcache_statements1;
  IF jsonb_extract_path_text(report_context, 'report_features', 'wait_sampling_tot')::boolean THEN
    DROP TABLE wait_sampling_total_stats1;
  END IF;
  IF num_nulls(start2_id, end2_id) = 0 THEN
    DROP TABLE top_statements2;
    DROP TABLE top_tables2;
    DROP TABLE top_indexes2;
    DROP TABLE top_io_tables2;
    DROP TABLE top_io_indexes2;
    DROP TABLE top_functions2;
    DROP TABLE top_kcache_statements2;
    IF jsonb_extract_path_text(report_context, 'report_features', 'wait_sampling_tot')::boolean THEN
      DROP TABLE wait_sampling_total_stats2;
    END IF;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION template_populate_sections(IN report_context jsonb, IN sserver_id integer,
  IN template text, IN report_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    c_sections CURSOR(init_depth integer) FOR
    WITH RECURSIVE search_tree(report_id, sect_id, parent_sect_id,
      toc_cap, tbl_cap, feature, function_name, href, content, depth,
      sect_ord) AS
    (
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.feature,
          rs.function_name,
          rs.href,
          rs.content,
          init_depth,
          ARRAY[s_ord]
        FROM report_struct rs
        WHERE rs.report_id = template_populate_sections.report_id AND parent_sect_id IS NULL
      UNION ALL
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.feature,
          rs.function_name,
          rs.href,
          rs.content,
          st.depth + 1,
          sect_ord || s_ord
        FROM report_struct rs JOIN search_tree st ON
          (rs.report_id, rs.parent_sect_id) =
          (st.report_id, st.sect_id)
    )
    SELECT * FROM search_tree ORDER BY sect_ord;

    toc_t       text := '';
    sect_t      text := '';
    tpl         text;
    cur_depth   integer := 1;
    func_output text := NULL;
    skip_depth  integer = 10;
BEGIN
    FOR r_result IN c_sections(2) LOOP
      ASSERT r_result.depth BETWEEN 1 AND 5, 'Section depth is not in 1 - 5';

      -- Check if section feature enabled in report
      IF r_result.depth > skip_depth THEN
        CONTINUE;
      ELSE
        skip_depth := 10;
      END IF;
      IF r_result.feature IS NOT NULL AND (
          NOT jsonb_extract_path_text(report_context, 'report_features', r_result.feature)::boolean
        OR (
            left(r_result.feature, 1) = '!' AND
            jsonb_extract_path_text(report_context, 'report_features', ltrim(r_result.feature,'!'))::boolean
          )
        )
      THEN
        skip_depth := r_result.depth;
        CONTINUE;
      END IF;

      IF r_result.depth != cur_depth THEN
        IF r_result.depth > cur_depth THEN
          toc_t := toc_t || repeat('<ul>', r_result.depth - cur_depth);
        END IF;
        IF r_result.depth < cur_depth THEN
          toc_t := toc_t || repeat('</ul>', cur_depth - r_result.depth);
        END IF;
        cur_depth := r_result.depth;
      END IF;

      func_output := '';

      -- Executing function of report section if requested
      IF r_result.function_name IS NOT NULL THEN
        IF (SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = 'pg_profile') THEN
          -- Fail when requested function doesn't exists in extension
          ASSERT (
            SELECT count(*) = 1
            FROM
              pg_catalog.pg_proc f JOIN pg_catalog.pg_depend dep
                ON (f.oid,'e') = (dep.objid, dep.deptype)
              JOIN pg_catalog.pg_extension ext
                ON (ext.oid = dep.refobjid)
            WHERE
              f.proname = r_result.function_name
              AND ext.extname = 'pg_profile'
              AND pg_catalog.pg_get_function_result(f.oid) =
                'text'
              AND pg_catalog.pg_get_function_arguments(f.oid) =
                'report_context jsonb, sserver_id integer'
            ),
            'Report requested function % not found', r_result.function_name;
        ELSE
          -- When not installed as an extension check only the function existance
          ASSERT (
            SELECT count(*) = 1
            FROM
              pg_catalog.pg_proc f
            WHERE
              f.proname = r_result.function_name
              AND pg_catalog.pg_get_function_result(f.oid) =
                'text'
              AND pg_catalog.pg_get_function_arguments(f.oid) =
                'report_context jsonb, sserver_id integer'
            ),
            format('Report requested function %s not found', r_result.function_name);
        END IF;

        -- Set report context
        IF r_result.href IS NOT NULL THEN
          report_context := jsonb_set(report_context, '{report_properties,sect_href}',
            to_jsonb(r_result.href));
        ELSE
          report_context := report_context #- '{report_properties,sect_href}';
        END IF;
        IF r_result.tbl_cap IS NOT NULL THEN
          report_context := jsonb_set(report_context, '{report_properties,sect_tbl_cap}',
            to_jsonb(r_result.tbl_cap));
        ELSE
          report_context := report_context #- '{report_properties,sect_tbl_cap}';
        END IF;

        ASSERT report_context IS NOT NULL, 'Lost report context';
        -- Execute function for our report and get a section
        EXECUTE format('SELECT %I($1,$2)', r_result.function_name)
        INTO func_output
        USING
          report_context,
          sserver_id
        ;
      END IF; -- report section contains a function

      -- Insert an entry to table of contents
      IF r_result.toc_cap IS NOT NULL AND trim(r_result.toc_cap) != '' THEN
        IF r_result.function_name IS NULL OR
          (func_output IS NOT NULL AND func_output != '') THEN
            toc_t := toc_t || format(
              '<li><a HREF="#%s">%s</a></li>',
              COALESCE(r_result.href, r_result.function_name),
              r_result.toc_cap
            );
        END IF;
      END IF;

      -- Adding table title
      IF r_result.function_name IS NULL OR
        (func_output IS NOT NULL AND func_output != '') THEN
        tpl := COALESCE(r_result.content, '');
        -- Processing section header
        IF r_result.tbl_cap IS NOT NULL THEN
          IF strpos(tpl, '{header}') > 0 THEN
            tpl := replace(
              tpl,
              '{header}',
              format(
                '<H%1$s><a NAME="%2$s">%3$s</a></H%1$s>',
                r_result.depth,
                COALESCE(r_result.href, r_result.function_name),
                r_result.tbl_cap
              )
            );
          ELSE
            tpl := format(
              '<H%1$s><a NAME="%2$s">%3$s</a></H%1$s>',
              r_result.depth,
              COALESCE(r_result.href, r_result.function_name),
              r_result.tbl_cap
            ) || tpl;
          END IF;
        END IF;

        -- Processing function output
        IF strpos(tpl, '{func_output}') > 0 THEN
          tpl := replace(tpl,
            '{func_output}',
            COALESCE(func_output, '')
          );
        ELSE
          tpl := tpl || COALESCE(func_output, '');
        END IF;
        sect_t := sect_t || tpl;
      END IF;

    END LOOP; -- Over recursive sections query

    -- Closing TOC <ul> tags based on final depth
    toc_t := toc_t || repeat('</ul>', cur_depth);

    template := replace(template, '{report:toc}', toc_t);
    template := replace(template, '{report:sect}', sect_t);

    RETURN template;
END;
$$ LANGUAGE plpgsql;
/* ===== Main report function ===== */

CREATE FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report          text;
    report_context  jsonb;
BEGIN
    -- Interval expanding in case of growth stats requested
    IF with_growth THEN
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start_id, end_id
        FROM get_sized_bounds(sserver_id, start_id, end_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start_id, end_id);
      END;
    END IF;

    -- Getting report context and check conditions
    report_context := get_report_context(sserver_id, start_id, end_id, description);

    -- Create internal temporary tables for report
    PERFORM init_report_temp_tables(report_context, sserver_id);

    -- Prepare report template
    report := get_report_template(report_context, 1);
    -- Populate template with report tables
    report := template_populate_sections(report_context, sserver_id, report, 1);
    /*
    * Cleanup cache temporary tables
    * This is needed to avoid conflict with existing table if several
    * reports are collected in one session
    */
    PERFORM cleanup_report_temp_tables(report_context, sserver_id);

    RETURN report;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server_id and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN server name, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id,
    description, with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server name and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report('local',start_id,end_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function for local server. Takes IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN sserver_id integer, IN time_range tstzrange,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(sserver_id, start_id, end_id, description, with_growth)
  FROM get_sampleids_by_timerange(sserver_id, time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN sserver_id integer, IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server ID and time interval.';

CREATE FUNCTION get_report(IN server name, IN time_range tstzrange,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id, description,with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server name and time interval.';

CREATE FUNCTION get_report(IN time_range tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(get_server_by_name('local'), start_id, end_id, description, with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name('local'), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function for local server. Takes time interval.';

CREATE FUNCTION get_report(IN server name, IN baseline varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id, description, with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN baseline varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function for server baseline. Takes server name and baseline name.';

CREATE FUNCTION get_report(IN baseline varchar(25), IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN get_report('local',baseline,description,with_growth);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION get_report(IN baseline varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function for local server baseline. Takes baseline name.';

CREATE FUNCTION get_report_latest(IN server name = NULL)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(srv.server_id, s.sample_id, e.sample_id, NULL)
  FROM samples s JOIN samples e ON (s.server_id = e.server_id AND s.sample_id = e.sample_id - 1)
    JOIN servers srv ON (e.server_id = srv.server_id AND e.sample_id = srv.last_sample_id)
  WHERE srv.server_name = COALESCE(server, 'local')
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report_latest(IN server name) IS 'Statistics report generation function for last two samples';
/* ===== Differential report functions ===== */

CREATE FUNCTION get_diffreport(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report          text;
    report_context  jsonb;
BEGIN
    -- Interval expanding in case of growth stats requested
    IF with_growth THEN
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start1_id, end1_id
        FROM get_sized_bounds(sserver_id, start1_id, end1_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start1_id, end1_id);
      END;
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start2_id, end2_id
        FROM get_sized_bounds(sserver_id, start2_id, end2_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start2_id, end2_id);
      END;
    END IF;

    -- Getting report context and check conditions
    report_context := get_report_context(sserver_id, start1_id, end1_id, description,
      start2_id, end2_id);

    -- Create internal temporary tables for report
    PERFORM init_report_temp_tables(report_context, sserver_id);

    -- Prepare report template
    report := get_report_template(report_context, 2);
    -- Populate template with report tables
    report := template_populate_sections(report_context, sserver_id, report, 2);
    /*
    * Cleanup cache temporary tables
    * This is needed to avoid conflict with existing table if several
    * reports are collected in one session
    */
    PERFORM cleanup_report_temp_tables(report_context, sserver_id);

    RETURN report;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_diffreport(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server_id and IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),start1_id,end1_id,
    start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',start1_id,end1_id,start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;

COMMENT ON FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN start2_id integer,IN end2_id integer, IN description text,
  IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN server name, IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline1) bl1
    CROSS JOIN get_baseline_samples(get_server_by_name(server), baseline2) bl2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline1 varchar(25),
  IN baseline2 varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and two baselines to compare.';

CREATE FUNCTION get_diffreport(IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',baseline1,baseline2,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes two baselines to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    start2_id,end2_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl1
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text,
  IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, reference baseline name as first interval, start and end sample_ids of second interval.';

CREATE FUNCTION get_diffreport(IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',baseline,
    start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes reference baseline name as first interval, start and end sample_ids of second interval.';

CREATE FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),start1_id,end1_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, start and end sample_ids of first interval and reference baseline name as second interval.';

CREATE FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',start1_id,end1_id,
    baseline,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline varchar(25), IN start2_id integer,
  IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes start and end sample_ids of first interval and reference baseline name as second interval.';

CREATE FUNCTION get_diffreport(IN server name, IN time_range1 tstzrange,
  IN time_range2 tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),tm1.start_id,tm1.end_id,
    tm2.start_id,tm2.end_id,description,with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range1) tm1
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range2) tm2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN time_range1 tstzrange,
  IN time_range2 tstzrange, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and two time intervals to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN time_range tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    tm2.start_id,tm2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl1
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range) tm2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN time_range tstzrange, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, baseline and time interval to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN time_range tstzrange,
  IN baseline varchar(25), IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),tm1.start_id,tm1.end_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl2
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range) tm1
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN time_range tstzrange,
  IN baseline varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, time interval and baseline to compare.';
