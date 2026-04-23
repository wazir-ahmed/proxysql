-- enable_galera_testing() populates runtime but not disk, so proxysql-tester.py's
-- LOAD ... FROM DISK wipes galera1/galera2/galera users and HG 2271-2294. Persist.
SAVE MYSQL USERS FROM RUNTIME;
SAVE MYSQL USERS TO DISK;
SAVE MYSQL SERVERS FROM RUNTIME;
SAVE MYSQL SERVERS TO DISK;
SAVE MYSQL VARIABLES FROM RUNTIME;
SAVE MYSQL VARIABLES TO DISK;

-- proxysql-ci.cnf pins sqliteserver-mysql_ifaces to 0.0.0.0:6030, overriding
-- TEST_GALERA's compiled-in default of 127.1.{1..3}.{11..}:3306. Bind 0.0.0.0:3306
-- so both the in-container Galera monitor and the simulator (proxysql:3306 from
-- the test-runner) reach the same sqliteserver.
SET sqliteserver-mysql_ifaces='0.0.0.0:3306';
LOAD SQLITESERVER VARIABLES TO RUNTIME;
SAVE SQLITESERVER VARIABLES TO DISK;
