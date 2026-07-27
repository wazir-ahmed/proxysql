/**
 * @file test_rds_bgd_repeated_deployment-t.cpp
 * @brief Reusing BGD hostgroups 1400-1403 for a second deployment.
 *
 * Steps:
 *
 * 1. Configure deployment A, complete writer and reader switchover, and publish
 *    empty topology.
 * 2. Verify that deployment A leaves no green pools or green routing pin.
 * 3. Replace the configured green servers with TLS-enabled deployment B.
 * 4. Verify that only deployment B membership, probes, pools, and routing are
 *    used during the second lifecycle.
 */

#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

#include "command_line.h"
#include "rds_bgd_tap.h"
#include "utils.h"

const uint32_t kTimeoutSeconds = 3;
const uint32_t kProbeTimeoutMs = 3000;
const uint32_t kNegativeProbeTimeoutMs = 500;

struct PoolState {
	int rc;
	string blue_echo;
	string green_writer_echo;
	string green_reader_echo;
	int64_t blue_count;
	int64_t green_writer_count;
	int64_t green_reader_count;
};

struct TestState {
	RDS_BGD_Cluster deployment_a { bgd_cluster_init() };
	RDS_BGD_Cluster deployment_b { bgd_cluster_1_deployment_b_init() };
	BGD_Hostgroups hostgroups { 1400, 1401, 1402, 1403 };
	vector<Endpoint> topology_endpoints { deployment_a.get_endpoints() };

	TestState() {
		vector<Endpoint> deployment_b_green = deployment_b.get_green_endpoints();
		topology_endpoints.insert(topology_endpoints.end(), deployment_b_green.begin(), deployment_b_green.end());
	}
};

int setup(CommandLine& cl, MYSQL*& admin, RDS_BGD_Simulator& sim) {
	if (cl.getEnv()) {
		diag("Error: failed to load TAP environment");
		return EXIT_FAILURE;
	}

	admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) {
		diag("Error: failed to connect to ProxySQL Admin");
		return EXIT_FAILURE;
	}

	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		diag("Error: failed to connect to the SQLite3-server simulator");
		mysql_close(admin);
		admin = nullptr;
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}

int cleanup(MYSQL* admin, RDS_BGD_Simulator& sim) {
	int admin_rc = bgd_admin_cleanup(admin);
	if (admin_rc != EXIT_SUCCESS) {
		diag("Error: failed to clean ProxySQL BGD test state");
	}
	mysql_close(admin);

	int simulator_rc = sim.cleanup();
	if (simulator_rc != EXIT_SUCCESS) {
		diag("Error: failed to clean SQLite3-server simulator state");
	}

	if (admin_rc != EXIT_SUCCESS || simulator_rc != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}

vector<RDS_BGD_Topology_Row> topology_with_reader_pair(RDS_BGD_Cluster& cluster, string status) {
	vector<RDS_BGD_Topology_Row> rows = cluster.get_topology(status);
	rows.push_back({
		cluster.blue_readers[0].hostname,
		cluster.blue_readers[0].hostname,
		cluster.blue_readers[0].port,
		"BLUE_GREEN_DEPLOYMENT_SOURCE",
		status,
	});
	rows.push_back({
		cluster.green_readers[0].hostname,
		cluster.green_readers[0].hostname,
		cluster.green_readers[0].port,
		"BLUE_GREEN_DEPLOYMENT_TARGET",
		status,
	});
	return rows;
}

vector<RDS_BGD_Topology_Row> target_only_completed(RDS_BGD_Cluster& cluster) {
	vector<RDS_BGD_Topology_Row> rows {{
		cluster.green_writer.hostname,
		cluster.green_writer.hostname,
		cluster.green_writer.port,
		"BLUE_GREEN_DEPLOYMENT_TARGET",
		"SWITCHOVER_COMPLETED",
	}};
	return rows;
}

int configure_read_only_values(RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster) {
	if (bgd_set_host_read_only_0(sim, cluster.blue_writer) != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}

	if (bgd_set_host_read_only_0(sim, cluster.green_writer) != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}

	if (bgd_set_host_read_only_1(sim, cluster.blue_readers[0]) != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}

	if (bgd_set_host_read_only_1(sim, cluster.green_readers[0]) != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}

int set_default_hostgroup(MYSQL* admin, int hostgroup) {
	vector<string> queries {
		"UPDATE mysql_users SET default_hostgroup=" + to_string(hostgroup) + " WHERE username='testuser'",
		"LOAD MYSQL USERS TO RUNTIME",
	};

	int rc = execute_all(admin, queries);
	return rc;
}

rc_t<string> connect_and_echo(CommandLine& cl) {
	MYSQL* client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	if (client == nullptr) {
		rc_t<string> result { EXIT_FAILURE, {} };
		return result;
	}

	auto result = bgd_backend_ip_echo(client);
	mysql_close(client);
	return result;
}

PoolState create_deployment_pools(CommandLine& cl, MYSQL* admin, BGD_Hostgroups& hg, RDS_BGD_Cluster& cluster) {
	PoolState pools { EXIT_FAILURE, {}, {}, {}, 0, 0, 0 };

	if (set_default_hostgroup(admin, hg.blue_writer) != EXIT_SUCCESS) {
		return pools;
	}

	auto [blue_rc, blue_echo] = connect_and_echo(cl);
	if (blue_rc != EXIT_SUCCESS) {
		return pools;
	}
	pools.blue_echo = blue_echo;

	if (set_default_hostgroup(admin, hg.green_writer) != EXIT_SUCCESS) {
		return pools;
	}

	auto [green_writer_rc, green_writer_echo] = connect_and_echo(cl);
	if (green_writer_rc != EXIT_SUCCESS) {
		return pools;
	}
	pools.green_writer_echo = green_writer_echo;

	if (set_default_hostgroup(admin, hg.green_reader) != EXIT_SUCCESS) {
		return pools;
	}

	auto [green_reader_rc, green_reader_echo] = connect_and_echo(cl);
	if (green_reader_rc != EXIT_SUCCESS) {
		return pools;
	}
	pools.green_reader_echo = green_reader_echo;

	if (set_default_hostgroup(admin, hg.blue_writer) != EXIT_SUCCESS) {
		return pools;
	}

	auto [blue_count_rc, blue_count] = bgd_connection_pool_count(admin, hg.blue_writer, cluster.blue_writer.hostname);
	if (blue_count_rc != EXIT_SUCCESS) {
		return pools;
	}
	pools.blue_count = blue_count;

	auto [writer_count_rc, writer_count] =
		bgd_connection_pool_count(admin, hg.green_writer, cluster.green_writer.hostname);
	if (writer_count_rc != EXIT_SUCCESS) {
		return pools;
	}
	pools.green_writer_count = writer_count;

	auto [reader_count_rc, reader_count] =
		bgd_connection_pool_count(admin, hg.green_reader, cluster.green_readers[0].hostname);
	if (reader_count_rc != EXIT_SUCCESS) {
		return pools;
	}
	pools.green_reader_count = reader_count;
	pools.rc = EXIT_SUCCESS;
	return pools;
}

bool pools_match(PoolState& pools, RDS_BGD_Cluster& cluster) {
	bool blue_matches = pools.blue_echo.find(cluster.blue_writer.ip) != string::npos;
	bool green_writer_matches = pools.green_writer_echo.find(cluster.green_writer.ip) != string::npos;
	bool green_reader_matches = pools.green_reader_echo.find(cluster.green_readers[0].ip) != string::npos;
	bool counts_match = pools.blue_count >= 1 && pools.green_writer_count >= 1 && pools.green_reader_count >= 1;

	bool matches =
		pools.rc == EXIT_SUCCESS && blue_matches && green_writer_matches && green_reader_matches && counts_match;
	return matches;
}

bool green_pools_are_zero(MYSQL* admin, BGD_Hostgroups& hg, RDS_BGD_Cluster& cluster) {
	auto [writer_rc, writer_count] =
		bgd_connection_pool_count(admin, hg.green_writer, cluster.green_writer.hostname);
	if (writer_rc != EXIT_SUCCESS) {
		return false;
	}

	auto [reader_rc, reader_count] =
		bgd_connection_pool_count(admin, hg.green_reader, cluster.green_readers[0].hostname);
	if (reader_rc != EXIT_SUCCESS) {
		return false;
	}

	bool pools_zero = writer_count == 0 && reader_count == 0;
	return pools_zero;
}

int wait_for_green_pools_zero(MYSQL* admin, BGD_Hostgroups& hg, RDS_BGD_Cluster& cluster) {
	string query =
		"SELECT "
		"(SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hg.green_writer) + " AND srv_host=" + bgd_sql_quote(cluster.green_writer.hostname) + ")=0 AND "
		"(SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hg.green_reader) + " AND srv_host=" + bgd_sql_quote(cluster.green_readers[0].hostname) + ")=0";

	int rc = bgd_wait_for_condition(admin, query, kTimeoutSeconds);
	return rc;
}

int wait_for_blue_pool_zero(MYSQL* admin, BGD_Hostgroups& hg, RDS_BGD_Cluster& cluster) {
	string query =
		"SELECT COALESCE(SUM(ConnUsed+ConnFree),0)=0 FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hg.blue_writer) + " AND srv_host=" + bgd_sql_quote(cluster.blue_writer.hostname);

	int rc = bgd_wait_for_condition(admin, query, kTimeoutSeconds);
	return rc;
}

bool runtime_green_membership_matches(
	MYSQL* admin, BGD_Hostgroups& hg, RDS_BGD_Cluster& present, RDS_BGD_Cluster& absent, int use_ssl)
{
	string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hg.green_writer) +
		" AND hostname=" + bgd_sql_quote(present.green_writer.hostname) + " AND port=3306 AND use_ssl=" +
		to_string(use_ssl) + ")=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hg.green_reader) +
		" AND hostname=" + bgd_sql_quote(present.green_readers[0].hostname) + " AND port=3306 AND use_ssl=" +
		to_string(use_ssl) + ")=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hg.green_writer) +
		" AND hostname=" + bgd_sql_quote(absent.green_writer.hostname) + " AND port=3306)=0 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hg.green_reader) +
		" AND hostname=" + bgd_sql_quote(absent.green_readers[0].hostname) + " AND port=3306)=0";

	auto [rc, rows] = mysql_query_ext_rows(admin, query);
	if (rc != EXIT_SUCCESS || rows.size() != 1 || rows[0].size() != 1) {
		return false;
	}

	bool matches = rows[0][0] == "1";
	return matches;
}

int replace_green_membership(
	MYSQL* admin, BGD_Hostgroups& hg, RDS_BGD_Cluster& old_deployment, RDS_BGD_Cluster& new_deployment)
{
	vector<string> delete_queries {
		"DELETE FROM mysql_servers WHERE hostgroup_id=" + to_string(hg.green_writer) +
			" AND hostname=" + bgd_sql_quote(old_deployment.green_writer.hostname) + " AND port=3306",
		"DELETE FROM mysql_servers WHERE hostgroup_id=" + to_string(hg.green_reader) +
			" AND hostname=" + bgd_sql_quote(old_deployment.green_readers[0].hostname) + " AND port=3306",
	};
	if (execute_all(admin, delete_queries) != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}

	vector<RDS_BGD_Host> green_servers { new_deployment.green_writer, new_deployment.green_readers[0] };
	if (bgd_admin_add_servers(admin, new_deployment, hg, green_servers, true, 1) != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}

	vector<string> load_queries { "LOAD MYSQL SERVERS TO RUNTIME" };
	int rc = execute_all(admin, load_queries);
	return rc;
}

int publish_writer_lifecycle(
	MYSQL* admin, RDS_BGD_Simulator& sim, TestState& state, RDS_BGD_Cluster& cluster, string deployment)
{
	vector<RDS_BGD_Topology_Row> initiated = topology_with_reader_pair(cluster, "SWITCHOVER_INITIATED");
	if (sim.topology_update(cluster.get_endpoints(), initiated) != EXIT_SUCCESS) {
		diag("Error: failed to publish SWITCHOVER_INITIATED topology for deployment %s", deployment.c_str());
		return EXIT_FAILURE;
	}

	if (bgd_wait_for_status(admin, state.hostgroups, "WRITER_SWITCHOVER_INITIATED", kTimeoutSeconds) != EXIT_SUCCESS) {
		diag("Error: BGD status for deployment %s did not reach WRITER_SWITCHOVER_INITIATED", deployment.c_str());
		return EXIT_FAILURE;
	}

	vector<RDS_BGD_Topology_Row> progress = topology_with_reader_pair(cluster, "SWITCHOVER_IN_PROGRESS");
	if (sim.topology_update(cluster.get_endpoints(), progress) != EXIT_SUCCESS) {
		diag("Error: failed to publish SWITCHOVER_IN_PROGRESS topology for deployment %s", deployment.c_str());
		return EXIT_FAILURE;
	}

	if (bgd_wait_for_status(admin, state.hostgroups, "WRITER_SWITCHOVER_IN_PROGRESS", kTimeoutSeconds) != EXIT_SUCCESS) {
		diag("Error: BGD status for deployment %s did not reach WRITER_SWITCHOVER_IN_PROGRESS", deployment.c_str());
		return EXIT_FAILURE;
	}

	vector<RDS_BGD_Topology_Row> post = topology_with_reader_pair(cluster, "SWITCHOVER_IN_POST_PROCESSING");
	if (sim.topology_update(cluster.get_endpoints(), post) != EXIT_SUCCESS) {
		diag("Error: failed to publish SWITCHOVER_IN_POST_PROCESSING topology for deployment %s", deployment.c_str());
		return EXIT_FAILURE;
	}

	int post_status_rc =
		bgd_wait_for_status(admin, state.hostgroups, "WRITER_SWITCHOVER_POST_PROCESSING", kTimeoutSeconds);
	if (post_status_rc != EXIT_SUCCESS) {
		diag("Error: BGD status for deployment %s did not reach WRITER_SWITCHOVER_POST_PROCESSING", deployment.c_str());
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}

int publish_reader_cleanup(
	MYSQL* admin, RDS_BGD_Simulator& sim, TestState& state, RDS_BGD_Cluster& cluster,
	string deployment, uint64_t& empty_sequence)
{
	vector<RDS_BGD_Topology_Row> completed = target_only_completed(cluster);
	if (sim.topology_update(cluster.get_endpoints(), completed) != EXIT_SUCCESS) {
		diag("Error: failed to publish target-only SWITCHOVER_COMPLETED for deployment %s", deployment.c_str());
		return EXIT_FAILURE;
	}

	if (bgd_wait_for_status(admin, state.hostgroups, "READER_SWITCHOVER_IN_PROGRESS", kTimeoutSeconds) != EXIT_SUCCESS) {
		diag("Error: BGD status for deployment %s did not reach READER_SWITCHOVER_IN_PROGRESS", deployment.c_str());
		return EXIT_FAILURE;
	}

	auto [seq_rc, seq] = sim.probe_log_last_sequence();
	if (seq_rc != EXIT_SUCCESS) {
		diag("Error: failed to read the probe sequence before empty topology for deployment %s", deployment.c_str());
		return EXIT_FAILURE;
	}
	empty_sequence = seq;

	if (sim.topology_delete(state.topology_endpoints) != EXIT_SUCCESS) {
		diag("Error: failed to publish empty topology for deployment %s", deployment.c_str());
		return EXIT_FAILURE;
	}

	if (bgd_wait_for_status(admin, state.hostgroups, "NONE", kTimeoutSeconds) != EXIT_SUCCESS) {
		diag("Error: BGD status for deployment %s did not reach NONE", deployment.c_str());
		return EXIT_FAILURE;
	}

	if (bgd_wait_for_server_placement(
			admin, state.hostgroups.blue_writer, state.hostgroups.blue_reader,
			cluster.blue_writer, false, kTimeoutSeconds
		) != EXIT_SUCCESS) {
		diag("Error: deployment %s did not restore the blue writer to hostgroup 1400", deployment.c_str());
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}

/**
 * Complete deployment A and remove its runtime-only effects.
 *
 * - Configure BGD hostgroups 1400-1403 with deployment A.
 * - Establish deployment A green pools and a green routing pin.
 * - Complete writer and reader switchover, then publish empty topology.
 * - Verify NONE, baseline blue placement, drained green pools, and resumed
 *   blue metadata probing.
 */
int test_deployment_a(CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim, TestState& state) {
	RDS_BGD_Cluster& deployment = state.deployment_a;
	BGD_Hostgroups& hg = state.hostgroups;

	int read_only_rc = configure_read_only_values(sim, deployment);
	if (read_only_rc != EXIT_SUCCESS) {
		diag("Error: failed to configure simulated read_only values for deployment A");
		return EXIT_FAILURE;
	}

	vector<RDS_BGD_Topology_Row> available = topology_with_reader_pair(deployment, "AVAILABLE");
	int topology_rc = sim.topology_update(deployment.get_endpoints(), available);
	if (topology_rc != EXIT_SUCCESS) {
		diag("Error: failed to publish AVAILABLE topology for deployment A");
		return EXIT_FAILURE;
	}

	vector<RDS_BGD_Host> blue_servers { deployment.blue_writer, deployment.blue_readers[0] };
	vector<RDS_BGD_Host> green_servers { deployment.green_writer, deployment.green_readers[0] };
	int admin_rc = bgd_admin_setup(
		admin, deployment, hg, BGD_Admin_Mode::explicit_configuration, blue_servers, green_servers, 0, 0
	);
	if (admin_rc != EXIT_SUCCESS) {
		diag("Error: failed to configure BGD hostgroups 1400-1403 for deployment A");
		return EXIT_FAILURE;
	}

	int available_rc = bgd_wait_for_status(admin, hg, "AVAILABLE", kTimeoutSeconds);
	if (available_rc != EXIT_SUCCESS) {
		diag("Error: BGD status for wHG 1400 did not reach AVAILABLE for deployment A");
		return EXIT_FAILURE;
	}

	PoolState pools = create_deployment_pools(cl, admin, hg, deployment);
	if (!pools_match(pools, deployment)) {
		diag("Error: failed to establish deployment A connection pools");
		return EXIT_FAILURE;
	}

	int writer_rc = publish_writer_lifecycle(admin, sim, state, deployment, "A");
	if (writer_rc != EXIT_SUCCESS) {
		diag("Error: failed to complete writer switchover for deployment A");
		return EXIT_FAILURE;
	}

	auto [route_rc, route] = connect_and_echo(cl);
	if (route_rc != EXIT_SUCCESS || route.find(deployment.green_writer.ip) == string::npos) {
		diag("Error: deployment A green routing pin was not created");
		return EXIT_FAILURE;
	}

	uint64_t empty_sequence = 0;
	int reader_rc = publish_reader_cleanup(admin, sim, state, deployment, "A", empty_sequence);
	if (reader_rc != EXIT_SUCCESS) {
		diag("Error: failed to complete reader switchover cleanup for deployment A");
		return EXIT_FAILURE;
	}

	ok(true, "deployment A cleanup sets BGD status for wHG 1400 to NONE and restores blue writer placement");

	int pools_rc = wait_for_green_pools_zero(admin, hg, deployment);
	if (pools_rc != EXIT_SUCCESS) {
		diag("Error: deployment A green connection pools were not drained");
		return EXIT_FAILURE;
	}

	auto [green_probe_rc, green_probe] = sim.wait_for_probe_log(
		empty_sequence, deployment.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0
	);
	if (green_probe_rc != EXIT_SUCCESS) {
		diag("Error: deployment A green metadata probe did not observe empty topology");
		return EXIT_FAILURE;
	}

	auto [blue_probe_rc, blue_probe] = sim.wait_for_probe_log(
		green_probe.sequence_id, deployment.blue_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0
	);
	if (blue_probe_rc != EXIT_SUCCESS) {
		diag("Error: blue metadata probing did not resume after deployment A cleanup");
		return EXIT_FAILURE;
	}

	auto [blue_route_rc, blue_route] = connect_and_echo(cl);
	bool blue_route_matches = blue_route_rc == EXIT_SUCCESS && blue_route.find(deployment.blue_writer.ip) != string::npos;
	bool probe_order_matches = green_probe.sequence_id < blue_probe.sequence_id;
	ok(blue_route_matches && probe_order_matches,
		"deployment A cleanup drains green pools, removes its green routing pin, and resumes blue metadata probing");
	return EXIT_SUCCESS;
}

/**
 * Reuse BGD hostgroups 1400-1403 for deployment B.
 *
 * - Replace deployment A green rows with TLS-enabled deployment B rows.
 * - Verify that metadata probes, runtime rows, and connection pools use only
 *   deployment B.
 * - Complete deployment B and verify that its green routing pin and pools are
 *   removed without recreating deployment A state.
 */
int test_deployment_b(CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim, TestState& state) {
	RDS_BGD_Cluster& deployment_a = state.deployment_a;
	RDS_BGD_Cluster& deployment_b = state.deployment_b;
	BGD_Hostgroups& hg = state.hostgroups;

	int read_only_rc = configure_read_only_values(sim, deployment_b);
	if (read_only_rc != EXIT_SUCCESS) {
		diag("Error: failed to configure simulated read_only values for deployment B");
		return EXIT_FAILURE;
	}

	auto [seq_rc, seq] = sim.probe_log_last_sequence();
	if (seq_rc != EXIT_SUCCESS) {
		diag("Error: failed to read the probe sequence before configuring deployment B");
		return EXIT_FAILURE;
	}

	int replace_rc = replace_green_membership(admin, hg, deployment_a, deployment_b);
	if (replace_rc != EXIT_SUCCESS) {
		diag("Error: failed to replace deployment A green rows with deployment B rows");
		return EXIT_FAILURE;
	}

	vector<RDS_BGD_Topology_Row> available = topology_with_reader_pair(deployment_b, "AVAILABLE");
	int topology_rc = sim.topology_update(deployment_b.get_endpoints(), available);
	if (topology_rc != EXIT_SUCCESS) {
		diag("Error: failed to publish AVAILABLE topology for deployment B");
		return EXIT_FAILURE;
	}

	int status_rc = bgd_wait_for_status(admin, hg, "AVAILABLE", kTimeoutSeconds);
	if (status_rc != EXIT_SUCCESS) {
		diag("Error: BGD status for wHG 1400 did not reach AVAILABLE for deployment B");
		return EXIT_FAILURE;
	}

	bool membership_matches = runtime_green_membership_matches(admin, hg, deployment_b, deployment_a, 1);
	ok(membership_matches, "runtime_mysql_servers contains only TLS-enabled deployment B green rows");

	auto [probe_rc, probe] = sim.wait_for_probe_log(
		seq, deployment_b.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 1
	);
	if (probe_rc != EXIT_SUCCESS) {
		diag("Error: deployment B green writer did not receive a TLS metadata probe");
		return EXIT_FAILURE;
	}

	int stale_probe_rc =
		bgd_expect_no_metadata_probe(sim, probe.sequence_id, deployment_a.green_writer.endpoint(), kNegativeProbeTimeoutMs);
	ok(stale_probe_rc == EXIT_SUCCESS,
		"deployment B metadata probing does not return to the removed deployment A green writer");

	PoolState pools = create_deployment_pools(cl, admin, hg, deployment_b);
	if (!pools_match(pools, deployment_b)) {
		diag("Error: failed to establish deployment B connection pools");
		return EXIT_FAILURE;
	}

	bool deployment_a_pools_zero = green_pools_are_zero(admin, hg, deployment_a);
	ok(deployment_a_pools_zero, "deployment B creates green pools without recreating deployment A pools");

	int writer_rc = publish_writer_lifecycle(admin, sim, state, deployment_b, "B");
	if (writer_rc != EXIT_SUCCESS) {
		diag("Error: failed to complete writer switchover for deployment B");
		return EXIT_FAILURE;
	}

	int blue_pool_rc = wait_for_blue_pool_zero(admin, hg, deployment_b);
	if (blue_pool_rc != EXIT_SUCCESS) {
		diag("Error: deployment B post-processing did not drain the blue writer pool");
		return EXIT_FAILURE;
	}

	auto [route_rc, route] = connect_and_echo(cl);
	bool route_matches = route_rc == EXIT_SUCCESS && route.find(deployment_b.green_writer.ip) != string::npos;
	ok(route_matches, "deployment B post-processing drains the blue pool and routes new connections to deployment B");

	uint64_t empty_sequence = 0;
	int reader_rc = publish_reader_cleanup(admin, sim, state, deployment_b, "B", empty_sequence);
	if (reader_rc != EXIT_SUCCESS) {
		diag("Error: failed to complete reader switchover cleanup for deployment B");
		return EXIT_FAILURE;
	}

	int pool_cleanup_rc = wait_for_green_pools_zero(admin, hg, deployment_b);
	if (pool_cleanup_rc != EXIT_SUCCESS) {
		diag("Error: deployment B green connection pools were not drained");
		return EXIT_FAILURE;
	}

	bool final_membership = runtime_green_membership_matches(admin, hg, deployment_b, deployment_a, 1);
	bool deployment_b_pools_zero = green_pools_are_zero(admin, hg, deployment_b);
	bool deployment_a_pools_still_zero = green_pools_are_zero(admin, hg, deployment_a);
	ok(final_membership && deployment_b_pools_zero && deployment_a_pools_still_zero,
		"deployment B cleanup retains only B rows and leaves no deployment A or deployment B green pools");
	return EXIT_SUCCESS;
}

int main() {
	plan(7);

	CommandLine cl {};
	MYSQL* admin = nullptr;
	RDS_BGD_Simulator sim {};

	if (setup(cl, admin, sim) != EXIT_SUCCESS) {
		return exit_status();
	}

	TestState state {};

	// Simulator: publish deployment A topology through writer and reader completion, then delete it.
	// ProxySQL: configure BGD hostgroups 1400-1403 and establish deployment A pools and routing.
	// Verify: deployment A cleanup restores blue routing and removes its green pools and routing pin.
	if (test_deployment_a(cl, admin, sim, state) != EXIT_SUCCESS) {
		goto exit_cleanup;
	}

	// Simulator: publish AVAILABLE through completion for deployment B on the same blue writer.
	// ProxySQL: replace deployment A green rows with TLS-enabled deployment B rows.
	// Verify: only deployment B membership, probes, pools, and routing are used by the second lifecycle.
	if (test_deployment_b(cl, admin, sim, state) != EXIT_SUCCESS) {
		goto exit_cleanup;
	}

exit_cleanup:
	if (cleanup(admin, sim) != EXIT_SUCCESS) {
		diag("Error: failed to clean the BGD TAP state");
		return EXIT_FAILURE;
	}
	return exit_status();
}
