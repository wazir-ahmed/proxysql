#!/usr/bin/env bash
set -e
# Let init_tsdb_variables() finish before pre-proxysql.sql pushes SAVE commands;
# otherwise SQLITE_LOCKED aborts ProxySQL (TEST_AURORA + PROXYSQL31 startup race).
sleep 5
