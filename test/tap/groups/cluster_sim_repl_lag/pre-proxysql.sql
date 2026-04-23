-- The simulator connects to the SQLite server as root/root; enable_replicationlag_testing()
-- does not provision any mysql_user, so inject one and persist.
INSERT OR REPLACE INTO mysql_users (username, password, default_hostgroup, active)
    VALUES ('root', 'root', 0, 1);
LOAD MYSQL USERS TO RUNTIME;
SAVE MYSQL USERS TO DISK;

-- proxysql-ci.cnf pins sqliteserver-mysql_ifaces to 0.0.0.0:6030, overriding
-- TEST_REPLICATIONLAG's compiled-in default of 0.0.0.0:3306. Restore 0.0.0.0:3306
-- so the simulator (proxysql:3306 from the test-runner) can reach sqliteserver.
SET sqliteserver-mysql_ifaces='0.0.0.0:3306';
LOAD SQLITESERVER VARIABLES TO RUNTIME;
SAVE SQLITESERVER VARIABLES TO DISK;
