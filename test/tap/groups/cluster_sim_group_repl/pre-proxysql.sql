-- enable_grouprep_testing() populates runtime but not disk, so proxysql-tester.py's
-- LOAD ... FROM DISK wipes grouprep1 and HG 3272-3274. Persist.
SAVE MYSQL USERS FROM RUNTIME;
SAVE MYSQL USERS TO DISK;
SAVE MYSQL SERVERS FROM RUNTIME;
SAVE MYSQL SERVERS TO DISK;
SAVE MYSQL VARIABLES FROM RUNTIME;
SAVE MYSQL VARIABLES TO DISK;

-- proxysql-ci.cnf pins sqliteserver-mysql_ifaces to 0.0.0.0:6030, overriding
-- TEST_GROUPREP's compiled-in default of 127.2.1.{0..49}:3306. Bind 0.0.0.0:3306
-- so both the in-container GroupRep monitor and the simulator (proxysql:3306
-- from the test-runner) reach the same sqliteserver.
SET sqliteserver-mysql_ifaces='0.0.0.0:3306';
LOAD SQLITESERVER VARIABLES TO RUNTIME;
SAVE SQLITESERVER VARIABLES TO DISK;
