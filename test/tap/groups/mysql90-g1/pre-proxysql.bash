#!/usr/bin/env bash
#
# change infra config
# inherits env from tester script
#

[[ $(mysql --skip-ssl-verify-server-cert -h 2>&1) =~ skip-ssl-verify-server-cert ]] || export SSLOPT=--skip-ssl-verify-server-cert

INFRA=infra-$(basename "$0" | sed 's/-g[0-9]//' | sed 's/_.*//')

# load environment for infra
source $JENKINS_SCRIPTS_PATH/${INFRA}/.env

# Start infra
$JENKINS_SCRIPTS_PATH/${INFRA}/docker-compose-init.bash

# make infra default
mysql ${SSLOPT} -h127.0.0.1 -P6032 -uadmin -padmin -e "
UPDATE mysql_users SET default_hostgroup=${WHG},comment='${INFRA}' WHERE username='root'; \
UPDATE mysql_users SET default_hostgroup=${WHG},comment='${INFRA}' WHERE username='user'; \
UPDATE mysql_users SET default_hostgroup=${WHG},comment='${INFRA}' WHERE username='testuser'; \
UPDATE mysql_users SET default_hostgroup=${WHG},comment='${INFRA}' WHERE username='sbtest1'; \
UPDATE mysql_users SET default_hostgroup=${WHG},comment='${INFRA}' WHERE username='sbtest2'; \
UPDATE mysql_users SET default_hostgroup=${WHG},comment='${INFRA}' WHERE username='sbtest3'; \
UPDATE mysql_users SET default_hostgroup=${WHG},comment='${INFRA}' WHERE username='sbtest4'; \
UPDATE mysql_users SET default_hostgroup=${WHG},comment='${INFRA}' WHERE username='ssluser'; \
LOAD MYSQL USERS TO RUNTIME; \
SAVE MYSQL USERS TO DISK; \
" 2>&1 | grep -vP 'mysql: .?Warning'

mysql ${SSLOPT} -h127.0.0.1 -P6032 -uadmin -padmin -e "
DELETE FROM mysql_servers WHERE hostgroup_id IN (0,1,2,3); \
INSERT INTO mysql_servers (hostgroup_id,hostname,port,max_replication_lag,comment) VALUES (0,'mysql1.${INFRA}',3306,1,'mysql1.${INFRA}'); \
INSERT INTO mysql_servers (hostgroup_id,hostname,port,max_replication_lag,comment) VALUES (1,'mysql1.${INFRA}',3306,1,'mysql1.${INFRA}'); \
INSERT INTO mysql_servers (hostgroup_id,hostname,port,max_replication_lag,comment) VALUES (1,'mysql2.${INFRA}',3306,1,'mysql2.${INFRA}'); \
#INSERT INTO mysql_servers (hostgroup_id,hostname,port,max_replication_lag,comment) VALUES (2,'mysql2.${INFRA}',3306,1,'mysql2.${INFRA}'); \
INSERT INTO mysql_servers (hostgroup_id,hostname,port,max_replication_lag,comment) VALUES (1,'mysql3.${INFRA}',3306,1,'mysql3.${INFRA}'); \
#INSERT INTO mysql_servers (hostgroup_id,hostname,port,max_replication_lag,comment) VALUES (2,'mysql3.${INFRA}',3306,1,'mysql3.${INFRA}'); \
LOAD MYSQL SERVERS TO RUNTIME; \
SAVE MYSQL SERVERS TO DISK; \
" 2>&1 | grep -vP 'mysql: .?Warning'

# wait for infra to stabilize
sleep 10
