# shellcheck shell=bash
# Aurora simulator TAP group environment

export CLUSTER_SIM_BINARY_PATH="${WORKSPACE}/test/deps/cluster_simulator/cluster_simulator"
export CLUSTER_SIM_TEST_PAYLOAD_PATH="${WORKSPACE}/test/deps/cluster_simulator/tests/aurora_tests_payloads"
export CLUSTER_SIM_HOST_FILE="${WORKSPACE}/test/tap/groups/aurora-sim/add-hosts"

# No backend infra: simulator drives ProxySQL directly through the admin port.
# Intentionally NOT setting DEFAULT_MYSQL_INFRA / DEFAULT_PGSQL_INFRA.
