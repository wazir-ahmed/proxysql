-- enable_aurora_testing() populates runtime but not disk, so proxysql-tester.py's
-- LOAD ... FROM DISK wipes aurora1/2/3 + HG 1271-1276. Persist runtime -> mem -> disk.
SAVE MYSQL USERS FROM RUNTIME;
SAVE MYSQL USERS TO DISK;
SAVE MYSQL SERVERS FROM RUNTIME;
SAVE MYSQL SERVERS TO DISK;
SAVE MYSQL QUERY RULES FROM RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;

-- proxysql-ci.cnf pins sqliteserver-mysql_ifaces to 0.0.0.0:6030, overriding
-- the TEST_AURORA compiled-in default (127.0.1.11:3306..127.0.3.20:3306). The
-- Aurora monitor inside ProxySQL connects to those aurora IPs on 3306 when it
-- queries REPLICA_HOST_STATUS, so without binding there auto-discovery never
-- lands and the simulator's init_state verifications fail. Bind 0.0.0.0:3306
-- so both the in-container monitor and the simulator (proxysql:3306 from the
-- test-runner) reach the same sqliteserver.
SET sqliteserver-mysql_ifaces='0.0.0.0:3306';
LOAD SQLITESERVER VARIABLES TO RUNTIME;
SAVE SQLITESERVER VARIABLES TO DISK;
