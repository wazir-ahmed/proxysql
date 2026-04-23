-- enable_aurora_testing() populates runtime but not disk, so proxysql-tester.py's
-- LOAD ... FROM DISK wipes aurora1/2/3 + HG 1271-1276. Persist runtime -> mem -> disk.
SAVE MYSQL USERS FROM RUNTIME;
SAVE MYSQL USERS TO DISK;
SAVE MYSQL SERVERS FROM RUNTIME;
SAVE MYSQL SERVERS TO DISK;
SAVE MYSQL QUERY RULES FROM RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
