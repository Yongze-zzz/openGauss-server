SET search_path TO information_schema;

DROP TABLE IF EXISTS ENGINES;
CREATE TABLE ENGINES (
  ENGINE varchar(64) NOT NULL DEFAULT '',
  SUPPORT varchar(8) NOT NULL DEFAULT '',
  COMMENT varchar(80) NOT NULL DEFAULT '',
  TRANSACTIONS varchar(3) DEFAULT NULL,
  XA varchar(3) DEFAULT NULL,
  SAVEPOINTS varchar(3) DEFAULT NULL
) WITH (orientation=row, compression=no);

insert into ENGINES values ('MEMORY', 'YES', 'Hash based, stored in memory, useful for temporary tables', 'NO', 'NO', 'NO');
insert into ENGINES values ('InnoDB', 'DEFAULT', 'Supports transactions, row-level locking, and foreign keys', 'YES', 'YES', 'YES');
insert into ENGINES values ('PERFORMANCE_SCHEMA', 'YES', 'Performance Schema', 'NO', 'NO', 'NO');
insert into ENGINES values ('MyISAM', 'YES', 'MyISAM storage engine', 'NO', 'NO', 'NO');
insert into ENGINES values ('ndbinfo', 'NO', 'MySQL Cluster system information storage engine', NULL, NULL, NULL);
insert into ENGINES values ('MRG_MYISAM', 'YES', 'Collection of identical MyISAM tables', 'NO', 'NO', 'NO');
insert into ENGINES values ('BLACKHOLE', 'YES', '/dev/null storage engine (anything you write to it disappears)', 'NO', 'NO', 'NO');
insert into ENGINES values ('CSV', 'YES', 'CSV storage engine', 'NO', 'NO', 'NO');
insert into ENGINES values ('ARCHIVE', 'YES', 'Archive storage engine', 'NO', 'NO', 'NO');
insert into ENGINES values ('ndbcluster', 'NO', 'Clustered, fault-tolerant tables', NULL, NULL, NULL);

GRANT SELECT ON ENGINES TO PUBLIC;

DROP TABLE IF EXISTS PROFILING;

CREATE TABLE PROFILING (
  QUERY_ID int NOT NULL DEFAULT '0',
  SEQ int NOT NULL DEFAULT '0',
  STATE varchar(30) NOT NULL DEFAULT '',
  DURATION decimal(9,6) NOT NULL DEFAULT '0.000000',
  CPU_USER decimal(9,6) DEFAULT NULL,
  CPU_SYSTEM decimal(9,6) DEFAULT NULL,
  CONTEXT_VOLUNTARY int DEFAULT NULL,
  CONTEXT_INVOLUNTARY int DEFAULT NULL,
  BLOCK_OPS_IN int DEFAULT NULL,
  BLOCK_OPS_OUT int DEFAULT NULL,
  MESSAGES_SENT int DEFAULT NULL,
  MESSAGES_RECEIVED int DEFAULT NULL,
  PAGE_FAULTS_MAJOR int DEFAULT NULL,
  PAGE_FAULTS_MINOR int DEFAULT NULL,
  SWAPS int DEFAULT NULL,
  SOURCE_FUNCTION varchar(30) DEFAULT NULL,
  SOURCE_FILE varchar(20) DEFAULT NULL,
  SOURCE_LINE int DEFAULT NULL
) WITH (orientation=row, compression=no);

GRANT SELECT ON PROFILING TO PUBLIC;

RESET search_path;
