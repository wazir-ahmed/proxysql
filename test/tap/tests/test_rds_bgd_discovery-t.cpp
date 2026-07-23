/**
 * @file test_rds_bgd_discovery-t.cpp
 * @brief Discovery ordering coverage for AWS RDS Blue/Green Deployments.
 */

#include <cstdlib>
#include <string>
#include <vector>

#include "command_line.h"
#include "rds_bgd_tap.h"
#include "utils.h"

namespace {

const uint32_t kTimeoutSeconds = 10;
const uint32_t kProbeTimeoutMs = 10000;

bool runtime_auto_row_matches(MYSQL* admin, const BGD_Hostgroups& hgs) {
	auto [rc, rows] = bgd_runtime_rows(admin, hgs.blue_writer);
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 6 &&
		rows[0][0] == to_string(hgs.blue_writer) && rows[0][1] == to_string(hgs.blue_reader) &&
		rows[0][2].empty() && rows[0][3].empty() && rows[0][4] == "1";
}

bool runtime_has_server(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host) {
	auto [rc, rows] = bgd_runtime_servers(admin, { hostgroup });
	if (rc != EXIT_SUCCESS) return false;
	for (const mysql_res_row& row : rows) {
		if (row.size() == 5 && row[0] == to_string(hostgroup) && row[1] == host.hostname &&
			row[2] == to_string(host.port) && row[3] == "ONLINE") return true;
	}
	return false;
}

bool no_persistent_bgd_row(MYSQL* admin, int writer_hostgroup) {
	auto [rc, rows] = mysql_query_ext_rows(
		admin, "SELECT COUNT(*) FROM mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" + to_string(writer_hostgroup));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

vector<Endpoint> topology_backends(RDS_BGD_Cluster& cluster, bool include_blue_readers) {
	vector<Endpoint> backends = cluster.get_writers();
	if (include_blue_readers) {
		for (RDS_BGD_Host& reader : cluster.blue_readers) backends.push_back(reader.endpoint());
	}
	return backends;
}

void set_read_only_writers(RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster) {
	for (Endpoint& writer : cluster.get_writer_hosts()) {
		if (sim.read_only_update(writer, false) != EXIT_SUCCESS) BAIL_OUT("failed to configure simulated writer read_only state");
	}
}

}  // namespace

int main() {
	plan(18);

	CommandLine cl {};
	if (cl.getEnv()) BAIL_OUT("failed to load TAP environment");
	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) BAIL_OUT("failed to connect to ProxySQL Admin");

	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}

	// Scenario 1: topology exists before the blue writer is added.
	RDS_BGD_Cluster first = bgd_cluster_init();
	BGD_Hostgroups first_hgs { 810, 811, 812, 813 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS) BAIL_OUT("failed to clean Admin state for topology-first discovery");
	set_read_only_writers(sim, first);
	int rc = sim.topology_update(first.get_writers(), first.get_topology("AVAILABLE"));
	ok(rc == EXIT_SUCCESS, "topology-first: publish recorded AVAILABLE observations before adding blue writer");
	if (rc != EXIT_SUCCESS) BAIL_OUT("failed to publish topology-first AVAILABLE observations");
	auto [first_seq_rc, first_seq] = sim.probe_log_last_sequence();
	if (first_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read topology-first probe sequence");
	if (bgd_admin_setup(admin, first, first_hgs, BGD_Admin_Mode::automatic, { first.blue_writer }, {}, 0) != EXIT_SUCCESS) {
		BAIL_OUT("failed to add topology-first blue writer");
	}
	rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=810 AND auto_generated=1",
		kTimeoutSeconds, sim, first_seq, "topology-first", "automatic discovery", "one automatic runtime row", first_hgs.blue_writer,
		{ first_hgs.blue_writer, first_hgs.blue_reader });
	ok(rc == EXIT_SUCCESS && runtime_auto_row_matches(admin, first_hgs),
		"topology-first: discovery derives blue hostgroups and stores NULL green hostgroups with auto_generated=1");
	auto [first_rows_rc, first_rows] = bgd_runtime_rows(admin, first_hgs.blue_writer);
	ok(first_rows_rc == EXIT_SUCCESS && first_rows.size() == 1 && no_persistent_bgd_row(admin, first_hgs.blue_writer),
		"topology-first: automatic discovery creates no duplicate runtime row or persistent BGD row");
	rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=810 AND status='AVAILABLE'",
		kTimeoutSeconds, sim, first_seq, "topology-first", "BGD worker", "AVAILABLE runtime state", first_hgs.blue_writer,
		{ first_hgs.blue_writer, first_hgs.blue_reader });
	int first_probe_rc = bgd_wait_for_probe(
		sim, first_seq, first.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0,
		admin, "topology-first", "BGD worker probe", first_hgs.blue_writer, { first_hgs.blue_writer, first_hgs.blue_reader });
	ok(rc == EXIT_SUCCESS && first_probe_rc == EXIT_SUCCESS,
		"topology-first: automatic worker reaches AVAILABLE and probes the recorded green writer over plaintext");
	MYSQL* first_client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	auto [first_echo_rc, first_echo] = first_client ? bgd_backend_ip_echo(first_client) : rc_t<string> { EXIT_FAILURE, {} };
	if (first_client) mysql_close(first_client);
	auto [first_pool_rc, first_pool] = bgd_connection_pool_count(admin, first_hgs.blue_writer, first.blue_writer.hostname);
	ok(first_echo_rc == EXIT_SUCCESS && first_echo.find(first.blue_writer.ip) != string::npos &&
		first_pool_rc == EXIT_SUCCESS && first_pool >= 1,
		"topology-first: client backend echo and pool state identify the configured blue writer");

	// Scenario 2: blue topology is configured before AWS exposes mysql.rds_topology.
	RDS_BGD_Cluster second = bgd_cluster_2_init();
	BGD_Hostgroups second_hgs { 820, 821, 822, 823 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS) BAIL_OUT("failed to clean Admin state for topology-absent discovery");
	set_read_only_writers(sim, second);
	rc = sim.topology_drop(topology_backends(second, true));
	ok(rc == EXIT_SUCCESS, "topology-absent: publish the recorded absent-topology condition");
	if (rc != EXIT_SUCCESS) BAIL_OUT("failed to publish topology-absent condition");
	auto [second_seq_rc, second_seq] = sim.probe_log_last_sequence();
	if (second_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read topology-absent probe sequence");
	if (bgd_admin_setup(admin, second, second_hgs, BGD_Admin_Mode::automatic,
		{ second.blue_writer, second.blue_readers[0], second.blue_readers[1] }, {}, 0) != EXIT_SUCCESS) {
		BAIL_OUT("failed to add topology-absent blue deployment");
	}
	int second_absent_probe_rc = bgd_wait_for_probe(
		sim, second_seq, second.blue_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0,
		admin, "topology-absent", "absence probe", second_hgs.blue_writer, { second_hgs.blue_writer, second_hgs.blue_reader });
	auto [second_absent_rows_rc, second_absent_rows] = bgd_runtime_rows(admin, second_hgs.blue_writer);
	ok(second_absent_probe_rc == EXIT_SUCCESS && no_persistent_bgd_row(admin, second_hgs.blue_writer) &&
		second_absent_rows_rc == EXIT_SUCCESS && second_absent_rows.empty(),
		"topology-absent: read-only discovery observes absence and creates no BGD row");
	auto [second_available_seq_rc, second_available_seq] = sim.probe_log_last_sequence();
	if (second_available_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read AVAILABLE publication probe sequence");
	rc = sim.topology_update(topology_backends(second, true), second.get_topology("AVAILABLE"));
	ok(rc == EXIT_SUCCESS, "topology-absent: publish recorded AVAILABLE observations after blue deployment exists");
	if (rc != EXIT_SUCCESS) BAIL_OUT("failed to publish topology-absent AVAILABLE observations");
	rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=820 AND auto_generated=1",
		kTimeoutSeconds, sim, second_available_seq, "topology-absent", "automatic discovery", "one derived runtime row", second_hgs.blue_writer,
		{ second_hgs.blue_writer, second_hgs.blue_reader });
	int second_probe_rc = bgd_wait_for_probe(
		sim, second_available_seq, second.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0,
		admin, "topology-absent", "BGD worker probe", second_hgs.blue_writer, { second_hgs.blue_writer, second_hgs.blue_reader });
	ok(rc == EXIT_SUCCESS && runtime_auto_row_matches(admin, second_hgs) && second_probe_rc == EXIT_SUCCESS,
		"topology-absent: AVAILABLE discovery derives the expected hostgroups and starts the green-writer probe");
	auto [second_count_rc, second_count_rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=820");
	ok(second_count_rc == EXIT_SUCCESS && second_count_rows.size() == 1 && second_count_rows[0].size() == 1 &&
		second_count_rows[0][0] == "1" && no_persistent_bgd_row(admin, second_hgs.blue_writer),
		"topology-absent: repeated discovery leaves one runtime-only BGD row");

	// Scenario 3: readers appear only after the automatic worker has started.
	RDS_BGD_Cluster third = bgd_cluster_3_init();
	BGD_Hostgroups third_hgs { 830, 831, 832, 833 };
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS) BAIL_OUT("failed to clean Admin state for late-reader discovery");
	set_read_only_writers(sim, third);
	rc = sim.topology_update(topology_backends(third, true), third.get_topology("AVAILABLE"));
	ok(rc == EXIT_SUCCESS, "late-readers: publish recorded AVAILABLE observations before starting automatic discovery");
	if (rc != EXIT_SUCCESS) BAIL_OUT("failed to publish late-reader AVAILABLE observations");
	auto [third_seq_rc, third_seq] = sim.probe_log_last_sequence();
	if (third_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read late-reader probe sequence");
	if (bgd_admin_setup(admin, third, third_hgs, BGD_Admin_Mode::automatic, { third.blue_writer }, {}, 0) != EXIT_SUCCESS) {
		BAIL_OUT("failed to add late-reader blue writer");
	}
	rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=830 AND status='AVAILABLE'",
		kTimeoutSeconds, sim, third_seq, "late-readers", "writer-only discovery", "AVAILABLE automatic worker", third_hgs.blue_writer,
		{ third_hgs.blue_writer, third_hgs.blue_reader });
	ok(rc == EXIT_SUCCESS && runtime_auto_row_matches(admin, third_hgs),
		"late-readers: writer-only automatic discovery creates the expected runtime row");
	auto [third_reader_seq_rc, third_reader_seq] = sim.probe_log_last_sequence();
	if (third_reader_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read late-reader baseline probe sequence");
	rc = bgd_admin_add_servers(admin, third, third_hgs, third.blue_readers, false, 1);
	if (rc == EXIT_SUCCESS) rc = execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" });
	ok(rc == EXIT_SUCCESS, "late-readers: add distinct-TLS blue readers and load them to runtime");
	rc = bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=2 FROM runtime_mysql_servers WHERE hostgroup_id=831 AND status='ONLINE'",
		kTimeoutSeconds, sim, third_reader_seq, "late-readers", "server reload", "both new blue readers ONLINE", third_hgs.blue_writer,
		{ third_hgs.blue_writer, third_hgs.blue_reader });
	ok(rc == EXIT_SUCCESS && runtime_has_server(admin, third_hgs.blue_reader, third.blue_readers[0]) &&
		runtime_has_server(admin, third_hgs.blue_reader, third.blue_readers[1]),
		"late-readers: runtime server state contains both newly loaded blue readers");
	vector<Endpoint> third_blue_backends { third.blue_writer.endpoint(), third.blue_readers[0].endpoint(), third.blue_readers[1].endpoint() };
	auto [third_restart_rc, third_restart_probe] = bgd_wait_for_probe_from_backends(
		sim, third_reader_seq, third_blue_backends, RDS_BGD_Probe_Kind::table_check, kProbeTimeoutMs);
	if (third_restart_rc != EXIT_SUCCESS) {
		bgd_timeout_diagnostics(admin, sim, third_reader_seq, "late-readers", "worker replacement",
			"a fresh blue-host table-check after the reader-set reload", third_hgs.blue_writer,
			{ third_hgs.blue_writer, third_hgs.blue_reader });
	}
	ok(third_restart_rc == EXIT_SUCCESS,
		"late-readers: reader-set reload replaces the pinned worker with a fresh blue-host topology check");
	const uint64_t third_replacement_baseline =
		third_restart_rc == EXIT_SUCCESS ? third_restart_probe.sequence_id : third_reader_seq;
	int third_reader_probe_rc = bgd_wait_for_probe(
		sim, third_replacement_baseline, third.blue_readers[0].endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 1,
		admin, "late-readers", "reader-set probe", third_hgs.blue_writer, { third_hgs.blue_writer, third_hgs.blue_reader });
	ok(third_reader_probe_rc == EXIT_SUCCESS,
		"late-readers: refreshed reader set is probed with the configured reader TLS value");
	int third_green_probe_rc = bgd_wait_for_probe(
		sim, third_replacement_baseline, third.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0,
		admin, "late-readers", "replacement resume", third_hgs.blue_writer, { third_hgs.blue_writer, third_hgs.blue_reader });
	ok(third_green_probe_rc == EXIT_SUCCESS,
		"late-readers: replacement resumes the green-writer probe after its fresh topology check");
	auto [third_bgd_rc, third_bgd_rows] = bgd_runtime_rows(admin, third_hgs.blue_writer);
	MYSQL* third_client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	auto [third_echo_rc, third_echo] = third_client ? bgd_backend_ip_echo(third_client) : rc_t<string> { EXIT_FAILURE, {} };
	if (third_client) mysql_close(third_client);
	auto [third_pool_rc, third_pool] = bgd_connection_pool_count(admin, third_hgs.blue_writer, third.blue_writer.hostname);
	ok(third_bgd_rc == EXIT_SUCCESS && third_bgd_rows.size() == 1 && runtime_auto_row_matches(admin, third_hgs) &&
		third_echo_rc == EXIT_SUCCESS && third_echo.find(third.blue_writer.ip) != string::npos &&
		third_pool_rc == EXIT_SUCCESS && third_pool >= 1,
		"late-readers: derived hostgroups remain singular and client pool routing stays on the blue writer");

	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS) diag("failed to clean final BGD TAP Admin state");
	mysql_close(admin);
	return exit_status();
}
