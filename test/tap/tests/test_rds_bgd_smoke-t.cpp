/**
 * @file test_rds_bgd_smoke-t.cpp
 * @brief Smoke test for TAP-controlled AWS RDS BGD simulation.
 *
 * Test coverage:
 * 1. Publishes an AVAILABLE topology for writable blue and green writers.
 * 2. Starts an explicitly configured BGD worker for the blue writer.
 * 3. Verifies the public AVAILABLE state and a plaintext metadata probe to
 *    the recorded green writer.
 */

#include <cstdlib>
#include <string>
#include <vector>

#include "rds_bgd_tap.h"
#include "command_line.h"
#include "utils.h"

int configure_explicit_bgd(MYSQL* admin, RDS_BGD_Cluster& c) {
	RDS_BGD_Host& writer = c.blue_writer;
	return execute_all(admin, {
		// Start from an empty BGD configuration owned by this test.
		"DELETE FROM mysql_servers",
		"DELETE FROM mysql_replication_hostgroups",
		"DELETE FROM mysql_aws_rds_bgd_hostgroups",

		// Register one active blue/green deployment and its blue writer.
		"INSERT INTO mysql_replication_hostgroups(writer_hostgroup,reader_hostgroup) "
			"VALUES (10,20)",
		"INSERT INTO mysql_aws_rds_bgd_hostgroups("
			"writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup,"
			"active,writer_is_also_reader,check_interval_ms,check_timeout_ms,comment) "
			"VALUES (10,20,30,40,1,0,100,800,'BGD simulator smoke test')",
		"INSERT INTO mysql_servers(hostgroup_id,hostname,port,use_ssl,comment) VALUES (10,'" +
			writer.hostname + "'," + std::to_string(writer.port) + ",0,'blue writer')",

		// Use the simulator credentials and start explicit monitoring.
		"SET mysql-monitor_username='testuser'",
		"SET mysql-monitor_password='testuser'",
		"SET mysql-monitor_enabled='true'",
		"SET mysql-aws_blue_green_deployment_auto_discovery='false'",
		"LOAD MYSQL VARIABLES TO RUNTIME",
		"LOAD MYSQL SERVERS TO RUNTIME",
	});
}

/**
 * Exercise the shortest successful BGD path from recorded AVAILABLE topology
 * through worker startup and direct green-writer probing.
 */
void test_available_discovery(MYSQL* admin, RDS_BGD_Simulator& sim) {
	RDS_BGD_Cluster c = bgd_cluster_init();

	// Both simulated writers must accept monitor probes before topology is published.
	for (Endpoint& writer : c.get_writer_hosts()) {
		if (sim.read_only_update(writer, false) != EXIT_SUCCESS) {
			BAIL_OUT("failed to configure writer read_only state");
		}
	}

	// Publish AVAILABLE topology before starting the worker, then retain the probe baseline.
	auto [seq_rc, seq] = sim.probe_log_last_sequence();
	if (seq_rc != EXIT_SUCCESS) {
		BAIL_OUT("failed to read the last BGD probe-log sequence");
	}
	int rc = sim.topology_update(c.get_writers(), c.get_topology("AVAILABLE"));
	ok(rc == EXIT_SUCCESS, "publish AVAILABLE topology to both writer IPs");
	if (rc != EXIT_SUCCESS) {
		BAIL_OUT("failed to publish BGD topology");
	}

	// Start explicit monitoring. The worker must publish its public AVAILABLE state.
	if (configure_explicit_bgd(admin, c) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure ProxySQL for BGD monitoring");
	}
	rc = wait_for_cond(
		admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups "
		"WHERE writer_hostgroup=10 AND status='AVAILABLE'",
		3);
	ok(rc == EXIT_SUCCESS, "ProxySQL enters the AVAILABLE BGD state");

	// Discovery then switches to the recorded green writer IP for the direct metadata probe.
	auto [probe_rc, green_probe] = sim.wait_for_probe_log(
		seq, c.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, 3000, 0);
	ok(probe_rc == EXIT_SUCCESS, "ProxySQL probes topology directly on the green writer IP over plaintext");
}

int main() {
	plan(3);

	CommandLine cl {};
	if (cl.getEnv()) {
		BAIL_OUT("failed to load TAP environment");
	}

	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) {
		BAIL_OUT("failed to connect to ProxySQL Admin");
	}

	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}
	if (bgd_register_test_cleanup(admin, sim) != EXIT_SUCCESS) BAIL_OUT("failed to register BGD TAP cleanup");

	test_available_discovery(admin, sim);

	int cleanup_rc = bgd_finish_test_cleanup(admin, sim);
	if (cleanup_rc != EXIT_SUCCESS) BAIL_OUT("failed to clean final smoke TAP state");
	mysql_close(admin);
	return exit_status();
}
