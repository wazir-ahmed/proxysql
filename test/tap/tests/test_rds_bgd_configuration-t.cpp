/**
 * @file test_rds_bgd_configuration-t.cpp
 * @brief Explicit AWS RDS Blue/Green configuration and persistence coverage.
 *
 * Test coverage:
 * 1. Starts workers when explicit rows and eligible servers arrive in either order.
 * 2. Accepts green membership before discovery, after discovery, or after worker startup.
 * 3. Converts an automatic runtime row into explicit persistent configuration.
 * 4. Validates required green hostgroups and SAVE-from-runtime behavior.
 */

#include <cerrno>
#include <cstdlib>
#include <string>
#include <vector>

#include "command_line.h"
#include "rds_bgd_tap.h"
#include "utils.h"

namespace {

const uint32_t kTimeoutSeconds = 3;
const uint32_t kProbeTimeoutMs = 3000;
const uint32_t kNoProbeTimeoutMs = 1200;

vector<Endpoint> cluster_backends(RDS_BGD_Cluster& cluster) {
	vector<Endpoint> backends { cluster.blue_writer.endpoint(), cluster.green_writer.endpoint() };
	for (RDS_BGD_Host& host : cluster.blue_readers) backends.push_back(host.endpoint());
	for (RDS_BGD_Host& host : cluster.green_readers) backends.push_back(host.endpoint());
	return backends;
}

vector<RDS_BGD_Topology_Row> topology_with_readers(RDS_BGD_Cluster& cluster, const string& status) {
	vector<RDS_BGD_Topology_Row> rows = cluster.get_topology(status);
	for (RDS_BGD_Host& host : cluster.blue_readers) {
		rows.push_back({ host.hostname, host.hostname, host.port, "BLUE_GREEN_DEPLOYMENT_SOURCE", status });
	}
	for (RDS_BGD_Host& host : cluster.green_readers) {
		rows.push_back({ host.hostname, host.hostname, host.port, "BLUE_GREEN_DEPLOYMENT_TARGET", status });
	}
	return rows;
}

void set_writers_writable(RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster) {
	for (Endpoint& writer : cluster.get_writer_hosts()) {
		if (sim.read_only_update(writer, false) != EXIT_SUCCESS) BAIL_OUT("failed to configure simulated writer read_only state");
	}
}

int insert_explicit_row(MYSQL* admin, const BGD_Hostgroups& hgs, const string& comment) {
	return execute_all(admin, {
		"INSERT INTO mysql_aws_rds_bgd_hostgroups("
		"writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup,"
		"active,writer_is_also_reader,check_interval_ms,check_timeout_ms,comment) VALUES (" +
		to_string(hgs.blue_writer) + "," + to_string(hgs.blue_reader) + "," +
		to_string(hgs.green_writer) + "," + to_string(hgs.green_reader) +
		",1,0,100,800," + bgd_sql_quote(comment) + ")"
	});
}

int configure_monitor(MYSQL* admin, const BGD_Hostgroups& hgs, bool automatic) {
	return execute_all(admin, {
		"INSERT INTO mysql_replication_hostgroups(writer_hostgroup,reader_hostgroup) VALUES (" +
			to_string(hgs.blue_writer) + "," + to_string(hgs.blue_reader) + ")",
		"SET mysql-monitor_username='testuser'",
		"SET mysql-monitor_password='testuser'",
		"SET mysql-monitor_enabled='true'",
		"SET mysql-monitor_read_only_interval=100",
		"SET mysql-monitor_aws_rds_topology_discovery_interval=1",
		"SET mysql-aws_blue_green_deployment_auto_discovery='" + string(automatic ? "true" : "false") + "'",
		"UPDATE mysql_users SET default_hostgroup=" + to_string(hgs.blue_writer) + " WHERE username='testuser'",
		"LOAD MYSQL VARIABLES TO RUNTIME",
		"LOAD MYSQL USERS TO RUNTIME",
		"LOAD MYSQL SERVERS TO RUNTIME",
	});
}

bool explicit_runtime_row_matches(MYSQL* admin, const BGD_Hostgroups& hgs, const string& status = "") {
	auto [rc, rows] = bgd_runtime_rows(admin, hgs.blue_writer);
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 6 &&
		rows[0][0] == to_string(hgs.blue_writer) && rows[0][1] == to_string(hgs.blue_reader) &&
		rows[0][2] == to_string(hgs.green_writer) && rows[0][3] == to_string(hgs.green_reader) &&
		rows[0][4] == "0" && (status.empty() || rows[0][5] == status);
}

bool runtime_membership_matches(MYSQL* admin, RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs) {
	auto [rc, rows] = bgd_runtime_servers(admin,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	if (rc != EXIT_SUCCESS || rows.size() != 6) return false;
	vector<pair<int, string>> expected {
		{ hgs.blue_writer, cluster.blue_writer.hostname },
		{ hgs.blue_reader, cluster.blue_readers[0].hostname },
		{ hgs.blue_reader, cluster.blue_readers[1].hostname },
		{ hgs.green_writer, cluster.green_writer.hostname },
		{ hgs.green_reader, cluster.green_readers[0].hostname },
		{ hgs.green_reader, cluster.green_readers[1].hostname },
	};
	for (const auto& item : expected) {
		bool found = false;
		for (const mysql_res_row& row : rows) {
			if (row.size() == 5 && row[0] == to_string(item.first) && row[1] == item.second &&
				row[2] == "3306" && row[3] == "ONLINE") {
				found = true;
				break;
			}
		}
		if (!found) return false;
	}
	return true;
}

bool persistent_row_matches(MYSQL* admin, const BGD_Hostgroups& hgs) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup "
		"FROM mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" + to_string(hgs.blue_writer));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 4 &&
		rows[0][0] == to_string(hgs.blue_writer) && rows[0][1] == to_string(hgs.blue_reader) &&
		rows[0][2] == to_string(hgs.green_writer) && rows[0][3] == to_string(hgs.green_reader);
}

bool persistent_row_absent(MYSQL* admin, int writer_hg) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" + to_string(writer_hg));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

void diag_unexpected_probe(const string& scenario, int rc, const RDS_BGD_Probe_Log& probe) {
	if (rc == ETIMEDOUT) return;
	if (rc != EXIT_SUCCESS) {
		diag("%s: negative worker-probe check returned rc=%d", scenario.c_str(), rc);
		return;
	}
	diag("%s: unexpected BGD probe sequence=%llu backend=%s:%d kind=%s", scenario.c_str(),
		static_cast<unsigned long long>(probe.sequence_id), probe.backend.host.c_str(), probe.backend.port,
		probe.probe_kind == RDS_BGD_Probe_Kind::table_check ? "table_check" : "metadata");
}

int add_all_servers(MYSQL* admin, const RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs) {
	return bgd_admin_add_servers(admin, cluster, hgs,
		{ cluster.blue_writer, cluster.blue_readers[0], cluster.blue_readers[1] }, false, 0) == EXIT_SUCCESS &&
		bgd_admin_add_servers(admin, cluster, hgs,
			{ cluster.green_writer, cluster.green_readers[0], cluster.green_readers[1] }, true, 1) == EXIT_SUCCESS &&
		execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" }) == EXIT_SUCCESS ? EXIT_SUCCESS : EXIT_FAILURE;
}

int wait_for_available(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs)
{
	int rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status='AVAILABLE'",
		kTimeoutSeconds, sim, sequence, scenario, "available", "explicit runtime row reaches AVAILABLE",
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	if (rc != EXIT_SUCCESS) return rc;
	auto [probe_rc, probe] = bgd_wait_for_probe(sim, sequence, cluster.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 1, admin, scenario, "green probe",
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	return probe_rc;
}

bool pool_and_backend_are_blue(const CommandLine& cl, MYSQL* admin, RDS_BGD_Cluster& cluster,
	const BGD_Hostgroups& hgs)
{
	MYSQL* client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	auto [echo_rc, echo] = client ? bgd_backend_ip_echo(client) : rc_t<string> { EXIT_FAILURE, {} };
	if (client) mysql_close(client);
	auto [pool_rc, pool] = bgd_connection_pool_count(admin, hgs.blue_writer, cluster.blue_writer.hostname);
	return echo_rc == EXIT_SUCCESS && echo.find(cluster.blue_writer.ip) != string::npos &&
		pool_rc == EXIT_SUCCESS && pool >= 1;
}

/**
 * Load an explicit BGD definition before eligible servers and verify that the
 * worker starts only after the blue server set is loaded.
 */
void test_explicit_before_servers(const CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Publish AVAILABLE, then load the explicit definition without eligible servers.
	RDS_BGD_Cluster first = bgd_cluster_init();
	BGD_Hostgroups first_hgs { 840, 841, 842, 843 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS || sim.topology_drop(cluster_backends(first)) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset explicit-before-servers scenario");
	}
	set_writers_writable(sim, first);
	int rc = sim.topology_update(cluster_backends(first), topology_with_readers(first, "AVAILABLE"));
	ok(rc == EXIT_SUCCESS, "explicit-before-servers: publish recorded AVAILABLE topology");
	if (rc != EXIT_SUCCESS || configure_monitor(admin, first_hgs, false) != EXIT_SUCCESS ||
		insert_explicit_row(admin, first_hgs, "explicit before servers") != EXIT_SUCCESS ||
		execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" }) != EXIT_SUCCESS) {
		BAIL_OUT("failed to load explicit configuration before servers");
	}
	auto [first_seq_rc, first_seq] = sim.probe_log_last_sequence();
	if (first_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read explicit-before-servers probe baseline");
	ok(explicit_runtime_row_matches(admin, first_hgs),
		"explicit-before-servers: runtime row records both configured green hostgroups with auto_generated=0");
	auto [first_no_probe_rc, first_no_probe] = bgd_wait_for_probe_from_backends(
		sim, first_seq, cluster_backends(first), RDS_BGD_Probe_Kind::table_check, kNoProbeTimeoutMs);
	diag_unexpected_probe("explicit-before-servers", first_no_probe_rc, first_no_probe);
	ok(first_no_probe_rc == ETIMEDOUT,
		"explicit-before-servers: no BGD worker probe starts before an eligible blue server exists");
	rc = add_all_servers(admin, first, first_hgs);
	auto [first_start_rc, first_start] = bgd_wait_for_probe(sim, first_seq, first.blue_writer.endpoint(),
		RDS_BGD_Probe_Kind::table_check, kProbeTimeoutMs, 0, admin, "explicit-before-servers", "blue-server load",
		first_hgs.blue_writer, { first_hgs.blue_writer, first_hgs.blue_reader, first_hgs.green_writer, first_hgs.green_reader });
	int first_available_rc = rc == EXIT_SUCCESS ? wait_for_available(admin, sim,
		first_start_rc == EXIT_SUCCESS ? first_start.sequence_id : first_seq, "explicit-before-servers", first, first_hgs) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && first_start_rc == EXIT_SUCCESS && first_available_rc == EXIT_SUCCESS &&
		explicit_runtime_row_matches(admin, first_hgs, "AVAILABLE") && runtime_membership_matches(admin, first, first_hgs) &&
		pool_and_backend_are_blue(cl, admin, first, first_hgs),
		"explicit-before-servers: blue-server load starts monitoring with the expected runtime membership, client backend, and pool");
}

/**
 * Load all servers before the explicit BGD definition and verify that the
 * definition load is the event that starts monitoring.
 */
void test_servers_before_explicit(const CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Load blue and green servers while the explicit BGD row is still absent.
	RDS_BGD_Cluster second = bgd_cluster_2_init();
	BGD_Hostgroups second_hgs { 850, 851, 852, 853 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS || sim.topology_drop(cluster_backends(second)) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset servers-before-explicit scenario");
	}
	set_writers_writable(sim, second);
	int rc = sim.topology_update(cluster_backends(second), topology_with_readers(second, "AVAILABLE"));
	if (rc != EXIT_SUCCESS || configure_monitor(admin, second_hgs, false) != EXIT_SUCCESS ||
		add_all_servers(admin, second, second_hgs) != EXIT_SUCCESS) BAIL_OUT("failed to add servers before explicit row");
	auto [second_seq_rc, second_seq] = sim.probe_log_last_sequence();
	if (second_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read servers-before-explicit probe baseline");
	auto [second_no_probe_rc, second_no_probe] = bgd_wait_for_probe_from_backends(
		sim, second_seq, cluster_backends(second), RDS_BGD_Probe_Kind::table_check, kNoProbeTimeoutMs);
	diag_unexpected_probe("servers-before-explicit", second_no_probe_rc, second_no_probe);
	ok(second_no_probe_rc == ETIMEDOUT,
		"servers-before-explicit: configured blue and green servers do not start the BGD worker before the explicit row loads");
	rc = insert_explicit_row(admin, second_hgs, "servers before explicit row");
	if (rc == EXIT_SUCCESS) rc = execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" });
	int second_available_rc = rc == EXIT_SUCCESS ? wait_for_available(admin, sim, second_seq,
		"servers-before-explicit", second, second_hgs) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && second_available_rc == EXIT_SUCCESS && explicit_runtime_row_matches(admin, second_hgs, "AVAILABLE") &&
		runtime_membership_matches(admin, second, second_hgs) && pool_and_backend_are_blue(cl, admin, second, second_hgs),
		"servers-before-explicit: loading the row starts the worker with explicit membership, blue routing, and pool state");
}

/**
 * Supply complete green membership before the first AVAILABLE observation and
 * verify that discovery converges on that membership.
 */
void test_green_members_before_available(MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Configure the complete explicit membership before publishing topology.
	RDS_BGD_Cluster third = bgd_cluster_3_init();
	BGD_Hostgroups third_hgs { 860, 861, 862, 863 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS || sim.topology_drop(cluster_backends(third)) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset green-before-available scenario");
	}
	set_writers_writable(sim, third);
	if (configure_monitor(admin, third_hgs, false) != EXIT_SUCCESS || insert_explicit_row(admin, third_hgs, "green before available") != EXIT_SUCCESS ||
		add_all_servers(admin, third, third_hgs) != EXIT_SUCCESS) BAIL_OUT("failed to configure green-before-available scenario");
	auto [third_seq_rc, third_seq] = sim.probe_log_last_sequence();
	if (third_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read green-before-available probe baseline");
	int rc = sim.topology_update(cluster_backends(third), topology_with_readers(third, "AVAILABLE"));
	int third_available_rc = rc == EXIT_SUCCESS ? wait_for_available(admin, sim, third_seq,
		"green-before-available", third, third_hgs) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && third_available_rc == EXIT_SUCCESS && runtime_membership_matches(admin, third, third_hgs),
		"green-before-available: members supplied before AVAILABLE converge on the complete runtime membership");
}

/**
 * Start discovery with blue membership only, then add green members and verify
 * that the active worker adopts the complete set.
 */
void test_green_members_after_discovery(MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Reach AVAILABLE with blue members before loading the configured green set.
	RDS_BGD_Cluster fourth = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups fourth_hgs { 870, 871, 872, 873 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS || sim.topology_drop(cluster_backends(fourth)) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset green-after-discovery scenario");
	}
	set_writers_writable(sim, fourth);
	int rc = sim.topology_update(cluster_backends(fourth), topology_with_readers(fourth, "AVAILABLE"));
	if (rc != EXIT_SUCCESS || configure_monitor(admin, fourth_hgs, false) != EXIT_SUCCESS ||
		insert_explicit_row(admin, fourth_hgs, "green after discovery") != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, fourth, fourth_hgs,
			{ fourth.blue_writer, fourth.blue_readers[0], fourth.blue_readers[1] }, false, 0) != EXIT_SUCCESS ||
		execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" }) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure green-after-discovery scenario");
	}
	auto [fourth_seq_rc, fourth_seq] = sim.probe_log_last_sequence();
	if (fourth_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read green-after-discovery probe baseline");
	int fourth_available_rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=870 AND status='AVAILABLE'",
		kTimeoutSeconds, sim, fourth_seq, "green-after-discovery", "initial discovery", "AVAILABLE explicit worker",
		fourth_hgs.blue_writer, { fourth_hgs.blue_writer, fourth_hgs.blue_reader, fourth_hgs.green_writer, fourth_hgs.green_reader });
	rc = bgd_admin_add_servers(admin, fourth, fourth_hgs,
		{ fourth.green_writer, fourth.green_readers[0], fourth.green_readers[1] }, true, 1);
	if (rc == EXIT_SUCCESS) rc = execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" });
	int fourth_membership_rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=3 FROM runtime_mysql_servers WHERE hostgroup_id IN (872,873) AND status='ONLINE'",
		kTimeoutSeconds, sim, fourth_seq, "green-after-discovery", "member load", "three green runtime members",
		fourth_hgs.blue_writer, { fourth_hgs.blue_writer, fourth_hgs.blue_reader, fourth_hgs.green_writer, fourth_hgs.green_reader });
	auto [fourth_green_probe_rc, fourth_green_probe] = bgd_wait_for_probe(sim, fourth_seq, fourth.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, -1, admin, "green-after-discovery", "membership reload",
		fourth_hgs.blue_writer, { fourth_hgs.blue_writer, fourth_hgs.blue_reader, fourth_hgs.green_writer, fourth_hgs.green_reader });
	ok(fourth_available_rc == EXIT_SUCCESS && rc == EXIT_SUCCESS && fourth_membership_rc == EXIT_SUCCESS &&
		fourth_green_probe_rc == EXIT_SUCCESS &&
		runtime_membership_matches(admin, fourth, fourth_hgs),
		"green-after-discovery: members added after discovery converge on the same complete runtime membership");
}

/**
 * Start an explicit worker against absent topology, then add green membership
 * before AVAILABLE and verify normal convergence.
 */
void test_green_members_after_worker_start(MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Start the worker with blue membership while topology and green members are absent.
	RDS_BGD_Cluster fifth = bgd_cluster_init();
	BGD_Hostgroups fifth_hgs { 880, 881, 882, 883 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS || sim.topology_drop(cluster_backends(fifth)) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset green-after-worker-start scenario");
	}
	set_writers_writable(sim, fifth);
	if (configure_monitor(admin, fifth_hgs, false) != EXIT_SUCCESS || insert_explicit_row(admin, fifth_hgs, "green after worker start") != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, fifth, fifth_hgs,
			{ fifth.blue_writer, fifth.blue_readers[0], fifth.blue_readers[1] }, false, 0) != EXIT_SUCCESS ||
		execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" }) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure green-after-worker-start scenario");
	}
	auto [fifth_seq_rc, fifth_seq] = sim.probe_log_last_sequence();
	if (fifth_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read green-after-worker-start probe baseline");
	auto [fifth_start_rc, fifth_start] = bgd_wait_for_probe(sim, fifth_seq, fifth.blue_writer.endpoint(),
		RDS_BGD_Probe_Kind::table_check, kProbeTimeoutMs, 0, admin, "green-after-worker-start", "worker start",
		fifth_hgs.blue_writer, { fifth_hgs.blue_writer, fifth_hgs.blue_reader, fifth_hgs.green_writer, fifth_hgs.green_reader });
	int rc = bgd_admin_add_servers(admin, fifth, fifth_hgs,
		{ fifth.green_writer, fifth.green_readers[0], fifth.green_readers[1] }, true, 1);
	if (rc == EXIT_SUCCESS) rc = execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" });
	int fifth_topology_rc = rc == EXIT_SUCCESS ? sim.topology_update(cluster_backends(fifth), topology_with_readers(fifth, "AVAILABLE")) : EXIT_FAILURE;
	int fifth_available_rc = fifth_topology_rc == EXIT_SUCCESS ? wait_for_available(admin, sim,
		fifth_start_rc == EXIT_SUCCESS ? fifth_start.sequence_id : fifth_seq, "green-after-worker-start", fifth, fifth_hgs) : EXIT_FAILURE;
	ok(fifth_start_rc == EXIT_SUCCESS && rc == EXIT_SUCCESS && fifth_topology_rc == EXIT_SUCCESS && fifth_available_rc == EXIT_SUCCESS &&
		runtime_membership_matches(admin, fifth, fifth_hgs),
		"green-after-worker-start: members added before switchover converge on the same complete runtime membership");
}

/**
 * Replace a nullable automatic runtime row with a complete user-owned row and
 * verify that the explicit values persist.
 */
void test_automatic_to_explicit_conversion(MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Discover the automatic row first, then disable discovery and load explicit values.
	RDS_BGD_Cluster sixth = bgd_cluster_2_init();
	BGD_Hostgroups sixth_hgs { 890, 891, 892, 893 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS || sim.topology_drop(cluster_backends(sixth)) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset automatic-conversion scenario");
	}
	set_writers_writable(sim, sixth);
	int rc = sim.topology_update(cluster_backends(sixth), topology_with_readers(sixth, "AVAILABLE"));
	if (rc != EXIT_SUCCESS || configure_monitor(admin, sixth_hgs, true) != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, sixth, sixth_hgs, { sixth.blue_writer }, false, 0) != EXIT_SUCCESS ||
		execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" }) != EXIT_SUCCESS) BAIL_OUT("failed to configure automatic conversion");
	auto [sixth_seq_rc, sixth_seq] = sim.probe_log_last_sequence();
	if (sixth_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read automatic conversion probe baseline");
	rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=890 "
		"AND auto_generated=1 AND green_writer_hostgroup IS NULL AND green_reader_hostgroup IS NULL",
		kTimeoutSeconds, sim, sixth_seq, "automatic-conversion", "automatic discovery", "nullable automatic runtime row",
		sixth_hgs.blue_writer, { sixth_hgs.blue_writer, sixth_hgs.blue_reader });
	ok(rc == EXIT_SUCCESS, "automatic-conversion: discovery records nullable green hostgroups only for the automatic runtime row");
	rc = execute_all(admin, { "SET mysql-aws_blue_green_deployment_auto_discovery='false'" });
	if (rc == EXIT_SUCCESS) rc = insert_explicit_row(admin, sixth_hgs, "converted automatic row");
	if (rc == EXIT_SUCCESS) rc = execute_all(admin, { "LOAD MYSQL VARIABLES TO RUNTIME", "LOAD MYSQL SERVERS TO RUNTIME" });
	int sixth_explicit_rc = rc == EXIT_SUCCESS ? bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=890 "
		"AND auto_generated=0 AND green_writer_hostgroup=892 AND green_reader_hostgroup=893",
		kTimeoutSeconds, sim, sixth_seq, "automatic-conversion", "explicit reload", "explicit green hostgroups replace automatic NULLs",
		sixth_hgs.blue_writer, { sixth_hgs.blue_writer, sixth_hgs.blue_reader, sixth_hgs.green_writer, sixth_hgs.green_reader }) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && sixth_explicit_rc == EXIT_SUCCESS && explicit_runtime_row_matches(admin, sixth_hgs) && persistent_row_matches(admin, sixth_hgs),
		"automatic-conversion: explicit configuration replaces nullable automatic values and persists as user state");
}

/**
 * Verify that persistent user rows require both green hostgroups while a
 * complete row loads into runtime as explicit configuration.
 */
void test_persistent_row_validation(MYSQL* admin) {
	// Attempt both invalid NULL variants before loading one complete row.
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS) BAIL_OUT("failed to reset persistent validation scenario");
	int null_writer_rc = mysql_query(admin,
		"INSERT INTO mysql_aws_rds_bgd_hostgroups(writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup) VALUES (900,901,NULL,903)");
	ok(null_writer_rc != 0 && persistent_row_absent(admin, 900),
		"persistent-validation: persistent user rows reject a NULL green writer hostgroup");
	int null_reader_rc = mysql_query(admin,
		"INSERT INTO mysql_aws_rds_bgd_hostgroups(writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup) VALUES (904,905,906,NULL)");
	ok(null_reader_rc != 0 && persistent_row_absent(admin, 904),
		"persistent-validation: persistent user rows reject a NULL green reader hostgroup");
	BGD_Hostgroups valid_hgs { 910, 911, 912, 913 };
	if (configure_monitor(admin, valid_hgs, false) != EXIT_SUCCESS || insert_explicit_row(admin, valid_hgs, "valid persistent row") != EXIT_SUCCESS ||
		execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" }) != EXIT_SUCCESS) BAIL_OUT("failed to load valid persistent row");
	ok(persistent_row_matches(admin, valid_hgs) && explicit_runtime_row_matches(admin, valid_hgs),
		"persistent-validation: a valid user row with both green hostgroups loads with auto_generated=0");
}

/**
 * SAVE BGD runtime state and verify that it recreates explicit configuration
 * without serializing an automatic discovery row.
 */
void test_save_from_runtime(MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Run explicit and automatic workers together, then remove the persistent explicit row.
	RDS_BGD_Cluster explicit_save = bgd_cluster_3_init();
	RDS_BGD_Cluster automatic_save = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups explicit_save_hgs { 920, 921, 922, 923 };
	BGD_Hostgroups automatic_save_hgs { 930, 931, 932, 933 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS || sim.topology_drop(cluster_backends(explicit_save)) != EXIT_SUCCESS ||
		sim.topology_drop(cluster_backends(automatic_save)) != EXIT_SUCCESS) BAIL_OUT("failed to reset SAVE scenario");
	set_writers_writable(sim, explicit_save);
	set_writers_writable(sim, automatic_save);
	if (sim.topology_update(cluster_backends(explicit_save), topology_with_readers(explicit_save, "AVAILABLE")) != EXIT_SUCCESS ||
		sim.topology_update(cluster_backends(automatic_save), topology_with_readers(automatic_save, "AVAILABLE")) != EXIT_SUCCESS ||
		configure_monitor(admin, explicit_save_hgs, true) != EXIT_SUCCESS ||
		insert_explicit_row(admin, explicit_save_hgs, "explicit save row") != EXIT_SUCCESS ||
		add_all_servers(admin, explicit_save, explicit_save_hgs) != EXIT_SUCCESS ||
		execute_all(admin, { "INSERT INTO mysql_replication_hostgroups(writer_hostgroup,reader_hostgroup) VALUES (930,931)" }) != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, automatic_save, automatic_save_hgs, { automatic_save.blue_writer }, false, 0) != EXIT_SUCCESS ||
		execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" }) != EXIT_SUCCESS) BAIL_OUT("failed to configure SAVE scenario");
	auto [save_seq_rc, save_seq] = sim.probe_log_last_sequence();
	if (save_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read SAVE scenario probe baseline");
	int automatic_runtime_rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=930 AND auto_generated=1",
		kTimeoutSeconds, sim, save_seq, "save-from-runtime", "automatic discovery", "automatic runtime row",
		automatic_save_hgs.blue_writer, { automatic_save_hgs.blue_writer, automatic_save_hgs.blue_reader });
	int explicit_runtime_rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=920 "
		"AND auto_generated=0 AND green_writer_hostgroup=922 AND green_reader_hostgroup=923",
		kTimeoutSeconds, sim, save_seq, "save-from-runtime", "explicit runtime row", "explicit runtime row before persistence reset",
		explicit_save_hgs.blue_writer, { explicit_save_hgs.blue_writer, explicit_save_hgs.blue_reader,
			explicit_save_hgs.green_writer, explicit_save_hgs.green_reader });
	int delete_explicit_rc = EXIT_FAILURE;
	bool explicit_absent_before_save = false;
	int save_rc = EXIT_FAILURE;
	if (automatic_runtime_rc == EXIT_SUCCESS && explicit_runtime_rc == EXIT_SUCCESS) {
		delete_explicit_rc = mysql_query(admin,
			"DELETE FROM mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" + to_string(explicit_save_hgs.blue_writer));
		explicit_absent_before_save = delete_explicit_rc == 0 && persistent_row_absent(admin, explicit_save_hgs.blue_writer);
		if (explicit_absent_before_save) save_rc = execute_all(admin, { "SAVE MYSQL SERVERS FROM RUNTIME" });
	}
	ok(automatic_runtime_rc == EXIT_SUCCESS && explicit_runtime_rc == EXIT_SUCCESS && delete_explicit_rc == 0 &&
		explicit_absent_before_save && save_rc == EXIT_SUCCESS && persistent_row_matches(admin, explicit_save_hgs) &&
		persistent_row_absent(admin, automatic_save_hgs.blue_writer),
		"save-from-runtime: SAVE recreates the missing explicit BGD row and skips the automatic runtime row");
}

}  // namespace

int main() {
	plan(15);

	CommandLine cl {};
	if (cl.getEnv()) BAIL_OUT("failed to load TAP environment");
	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) BAIL_OUT("failed to connect to ProxySQL Admin");
	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}
	if (bgd_register_test_cleanup(admin, sim) != EXIT_SUCCESS) BAIL_OUT("failed to register BGD TAP cleanup");

	test_explicit_before_servers(cl, admin, sim);
	test_servers_before_explicit(cl, admin, sim);
	test_green_members_before_available(admin, sim);
	test_green_members_after_discovery(admin, sim);
	test_green_members_after_worker_start(admin, sim);
	test_automatic_to_explicit_conversion(admin, sim);
	test_persistent_row_validation(admin);
	test_save_from_runtime(admin, sim);

	int cleanup_rc = bgd_finish_test_cleanup(admin, sim);
	if (cleanup_rc != EXIT_SUCCESS) BAIL_OUT("failed to clean final BGD TAP state");
	mysql_close(admin);
	return exit_status();
}
