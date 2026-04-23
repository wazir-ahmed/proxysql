#!/usr/bin/env bash
set -e
# Let init_tsdb_variables() settle before admin SAVE commands land; otherwise
# TEST_READONLY + PROXYSQL31 race trips SQLITE_LOCKED and aborts ProxySQL.
sleep 5
