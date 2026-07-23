/**
 * @file test_rds_bgd_lifecycle-t.cpp
 * @brief Normal AWS RDS Blue/Green forward lifecycle coverage.
 */

#include <cstdlib>
#include <string>
#include <vector>

#include "command_line.h"
#include "rds_bgd_tap.h"
#include "utils.h"

namespace {

const uint32_t kTimeoutSeconds = 3;
const uint32_t kProbeTimeoutMs = 3000;
const char* kScenario = "normal-forward-lifecycle";

vector<Endpoint> topology_backends(RDS_BGD_Cluster& cluster) {
	return { cluster.blue_writer.endpoint(), cluster.green_writer.endpoint() };
}

vector<RDS_BGD_Topology_Row> topology_with_one_reader_pair(RDS_BGD_Cluster& cluster, const string& status) {
	vector<RDS_BGD_Topology_Row> rows = cluster.get_topology(status);
	rows.push_back({ cluster.blue_readers[0].hostname, cluster.blue_readers[0].hostname,
		cluster.blue_readers[0].port, "BLUE_GREEN_DEPLOYMENT_SOURCE", status });
	rows.push_back({ cluster.green_readers[0].hostname, cluster.green_readers[0].hostname,
		cluster.green_readers[0].port, "BLUE_GREEN_DEPLOYMENT_TARGET", status });
	return rows;
}

vector<RDS_BGD_Topology_Row> target_only_completed(RDS_BGD_Cluster& cluster) {
	return {{ cluster.green_writer.hostname, cluster.green_writer.hostname, cluster.green_writer.port,
		"BLUE_GREEN_DEPLOYMENT_TARGET", "SWITCHOVER_COMPLETED" }};
}

void set_read_only(RDS_BGD_Simulator& sim, RDS_BGD_Host& host, bool value) {
	if (sim.read_only_update(host.host_endpoint(), value) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure simulated read_only state");
	}
}

int scenario_cleanup(MYSQL* admin, RDS_BGD_Simulator& sim, const vector<Endpoint>& backends) {
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS) return EXIT_FAILURE;
	return sim.topology_drop(backends);
}

int wait_for_status(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const BGD_Hostgroups& hgs, const string& phase, const string& status)
{
	return bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status=" + bgd_sql_quote(status),
		kTimeoutSeconds, sim, sequence, kScenario, phase, "runtime BGD status " + status,
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

int wait_for_observation(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs, const string& phase)
{
	auto [rc, probe] = bgd_wait_for_probe(sim, sequence, cluster.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin, kScenario, phase,
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	return rc;
}

bool server_has_status(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, const string& status) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT status FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=" + to_string(host.port));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == status;
}

bool server_absent(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=" + to_string(host.port));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

bool writer_in_expected_placement(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster,
	bool in_writer, bool in_reader)
{
	return (in_writer ? server_has_status(admin, hgs.blue_writer, cluster.blue_writer, "ONLINE")
		: server_absent(admin, hgs.blue_writer, cluster.blue_writer)) &&
		(in_reader ? server_has_status(admin, hgs.blue_reader, cluster.blue_writer, "ONLINE")
		: server_absent(admin, hgs.blue_reader, cluster.blue_writer));
}

int64_t pool_count_for_host(MYSQL* admin, const string& hostname) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE srv_host=" +
		bgd_sql_quote(hostname));
	if (rc != EXIT_SUCCESS || rows.size() != 1 || rows[0].size() != 1) return -1;
	return strtoll(rows[0][0].c_str(), nullptr, 10);
}

int64_t last_read_only_log_time(MYSQL* admin, const RDS_BGD_Host& host) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COALESCE(MAX(time_start_us),0) FROM mysql_server_read_only_log WHERE hostname=" +
		bgd_sql_quote(host.hostname) + " AND port=" + to_string(host.port));
	if (rc != EXIT_SUCCESS || rows.size() != 1 || rows[0].size() != 1) return -1;
	return strtoll(rows[0][0].c_str(), nullptr, 10);
}

bool no_read_only_log_after(MYSQL* admin, const RDS_BGD_Host& host, int64_t baseline) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM mysql_server_read_only_log WHERE hostname=" + bgd_sql_quote(host.hostname) +
		" AND port=" + to_string(host.port) + " AND time_start_us>" + to_string(baseline));
	return baseline >= 0 && rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

int set_default_hostgroup(MYSQL* admin, int hostgroup) {
	return execute_all(admin, {
		"UPDATE mysql_users SET default_hostgroup=" + to_string(hostgroup) + " WHERE username='testuser'",
		"LOAD MYSQL USERS TO RUNTIME",
	});
}

rc_t<string> connect_and_echo(const CommandLine& cl) {
	MYSQL* client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	if (client == nullptr) return { EXIT_FAILURE, {} };
	auto result = bgd_backend_ip_echo(client);
	mysql_close(client);
	return result;
}

bool green_rows_remain_online(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster) {
	return server_has_status(admin, hgs.green_writer, cluster.green_writer, "ONLINE") &&
		server_has_status(admin, hgs.green_reader, cluster.green_readers[0], "ONLINE");
}

}  // namespace

int main() {
	plan(24);

	CommandLine cl {};
	if (cl.getEnv()) BAIL_OUT("failed to load TAP environment");
	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) BAIL_OUT("failed to connect to ProxySQL Admin");

	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}

	RDS_BGD_Cluster cluster = bgd_cluster_init();
	BGD_Hostgroups hgs { 970, 971, 972, 973 };
	vector<Endpoint> backends = topology_backends(cluster);
	if (scenario_cleanup(admin, sim, backends) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to reset normal lifecycle scenario");
	}

	set_read_only(sim, cluster.blue_writer, false);
	set_read_only(sim, cluster.green_writer, false);
	set_read_only(sim, cluster.blue_readers[0], true);
	set_read_only(sim, cluster.blue_readers[1], true);
	set_read_only(sim, cluster.green_readers[0], true);
	set_read_only(sim, cluster.green_readers[1], true);

	if (bgd_admin_setup(admin, cluster, hgs, BGD_Admin_Mode::explicit_configuration,
		{ cluster.blue_writer, cluster.blue_readers[0], cluster.blue_readers[1] },
		{ cluster.green_writer, cluster.green_readers[0] }) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to configure normal lifecycle scenario");
	}

	auto [available_seq_rc, available_seq] = sim.probe_log_last_sequence();
	if (available_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read AVAILABLE probe baseline");
	int rc = sim.topology_update(backends, topology_with_one_reader_pair(cluster, "AVAILABLE"));
	int available_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, available_seq, hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && available_rc == EXIT_SUCCESS,
		"recorded AVAILABLE observation enters AVAILABLE runtime status");

	int blue_connect_rc = connect_and_echo(cl).first;
	int green_writer_pool_rc = set_default_hostgroup(admin, hgs.green_writer) == EXIT_SUCCESS ? connect_and_echo(cl).first : EXIT_FAILURE;
	int green_reader_pool_rc = set_default_hostgroup(admin, hgs.green_reader) == EXIT_SUCCESS ? connect_and_echo(cl).first : EXIT_FAILURE;
	int restore_blue_rc = set_default_hostgroup(admin, hgs.blue_writer);
	auto [blue_pool_count_rc, blue_pool] = bgd_connection_pool_count(admin, hgs.blue_writer, cluster.blue_writer.hostname);
	auto [green_writer_pool_count_rc, green_writer_pool] = bgd_connection_pool_count(admin, hgs.green_writer);
	auto [green_reader_pool_count_rc, green_reader_pool] = bgd_connection_pool_count(admin, hgs.green_reader);
	ok(blue_connect_rc == EXIT_SUCCESS && blue_pool_count_rc == EXIT_SUCCESS && blue_pool >= 1 &&
		green_writer_pool_rc == EXIT_SUCCESS && green_writer_pool_count_rc == EXIT_SUCCESS && green_writer_pool >= 1 &&
		green_reader_pool_rc == EXIT_SUCCESS && green_reader_pool_count_rc == EXIT_SUCCESS && green_reader_pool >= 1 &&
		restore_blue_rc == EXIT_SUCCESS,
		"AVAILABLE establishes blue and eligible green connection pools before lifecycle effects");

	auto [available_repeat_seq_rc, available_repeat_seq] = sim.probe_log_last_sequence();
	rc = available_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_one_reader_pair(cluster, "AVAILABLE")) : EXIT_FAILURE;
	int available_repeat_rc = rc == EXIT_SUCCESS ? wait_for_observation(admin, sim, available_repeat_seq, cluster, hgs, "available repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && available_repeat_rc == EXIT_SUCCESS &&
		writer_in_expected_placement(admin, hgs, cluster, true, false),
		"repeated AVAILABLE observation preserves status and writer placement");

	auto [initiated_seq_rc, initiated_seq] = sim.probe_log_last_sequence();
	rc = initiated_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_one_reader_pair(cluster, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	int initiated_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, initiated_seq, hgs, "initiated", "WRITER_SWITCHOVER_INITIATED") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_rc == EXIT_SUCCESS,
		"recorded SWITCHOVER_INITIATED observation enters initiated runtime status");

	int64_t initiated_writer_log = last_read_only_log_time(admin, cluster.blue_writer);
	int64_t initiated_reader_log = last_read_only_log_time(admin, cluster.blue_readers[0]);
	set_read_only(sim, cluster.blue_writer, true);
	set_read_only(sim, cluster.blue_readers[0], false);
	auto [initiated_suppression_seq_rc, initiated_suppression_seq] = sim.probe_log_last_sequence();
	int initiated_suppression_rc = initiated_suppression_seq_rc == EXIT_SUCCESS ?
		wait_for_observation(admin, sim, initiated_suppression_seq, cluster, hgs, "initiated suppression") : EXIT_FAILURE;
	ok(initiated_suppression_rc == EXIT_SUCCESS && writer_in_expected_placement(admin, hgs, cluster, true, false) &&
		server_has_status(admin, hgs.blue_reader, cluster.blue_readers[0], "ONLINE") &&
		server_absent(admin, hgs.blue_writer, cluster.blue_readers[0]) &&
		no_read_only_log_after(admin, cluster.blue_writer, initiated_writer_log) &&
		no_read_only_log_after(admin, cluster.blue_readers[0], initiated_reader_log),
		"initiated suppresses read-only placement changes for deployment members");

	auto [initiated_repeat_seq_rc, initiated_repeat_seq] = sim.probe_log_last_sequence();
	rc = initiated_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_one_reader_pair(cluster, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	int initiated_repeat_rc = rc == EXIT_SUCCESS ? wait_for_observation(admin, sim, initiated_repeat_seq, cluster, hgs, "initiated repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(admin, sim, initiated_repeat_seq, hgs, "initiated repeat", "WRITER_SWITCHOVER_INITIATED") == EXIT_SUCCESS &&
		writer_in_expected_placement(admin, hgs, cluster, true, false),
		"repeated initiated observation preserves status and suppressed placement");

	auto [progress_seq_rc, progress_seq] = sim.probe_log_last_sequence();
	rc = progress_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_one_reader_pair(cluster, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	int progress_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, progress_seq, hgs, "in progress", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && progress_rc == EXIT_SUCCESS,
		"recorded SWITCHOVER_IN_PROGRESS observation enters in-progress runtime status");

	ok(writer_in_expected_placement(admin, hgs, cluster, false, true) &&
		server_has_status(admin, hgs.blue_reader, cluster.blue_readers[0], "ONLINE") &&
		server_absent(admin, hgs.blue_writer, cluster.blue_readers[0]),
		"in-progress policy demotes the mapped blue writer while suppression keeps the blue reader placed");

	auto [progress_suppression_seq_rc, progress_suppression_seq] = sim.probe_log_last_sequence();
	int progress_suppression_rc = progress_suppression_seq_rc == EXIT_SUCCESS ?
		wait_for_observation(admin, sim, progress_suppression_seq, cluster, hgs, "in-progress suppression") : EXIT_FAILURE;
	ok(progress_suppression_rc == EXIT_SUCCESS && server_has_status(admin, hgs.blue_reader, cluster.blue_readers[0], "ONLINE") &&
		server_absent(admin, hgs.blue_writer, cluster.blue_readers[0]) &&
		no_read_only_log_after(admin, cluster.blue_readers[0], initiated_reader_log),
		"in-progress continues to suppress the pending read-only promotion");

	auto [progress_repeat_seq_rc, progress_repeat_seq] = sim.probe_log_last_sequence();
	rc = progress_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_one_reader_pair(cluster, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	int progress_repeat_rc = rc == EXIT_SUCCESS ? wait_for_observation(admin, sim, progress_repeat_seq, cluster, hgs, "in-progress repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && progress_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(admin, sim, progress_repeat_seq, hgs, "in-progress repeat", "WRITER_SWITCHOVER_IN_PROGRESS") == EXIT_SUCCESS &&
		writer_in_expected_placement(admin, hgs, cluster, false, true),
		"repeated in-progress observation preserves status and blue-writer demotion");

	auto [post_seq_rc, post_seq] = sim.probe_log_last_sequence();
	rc = post_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_one_reader_pair(cluster, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	int post_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, post_seq, hgs, "post processing", "WRITER_SWITCHOVER_POST_PROCESSING") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && post_rc == EXIT_SUCCESS,
		"recorded SWITCHOVER_IN_POST_PROCESSING observation enters post-processing runtime status");

	ok(writer_in_expected_placement(admin, hgs, cluster, true, false) &&
		server_has_status(admin, hgs.blue_reader, cluster.blue_readers[0], "ONLINE") &&
		server_has_status(admin, hgs.blue_reader, cluster.blue_readers[1], "SHUNNED_AWS_BGD"),
		"post-processing restores writer placement and shuns the unmatched blue reader");

	ok(pool_count_for_host(admin, cluster.blue_writer.hostname) == 0,
		"post-processing drains existing pools for the mapped blue writer hostname");

	auto [post_echo_rc, post_echo] = connect_and_echo(cl);
	ok(post_echo_rc == EXIT_SUCCESS && post_echo.find(cluster.green_writer.ip) != string::npos,
		"post-processing pins a new mapped-blue writer connection to the green backend IP");

	auto [post_suppression_seq_rc, post_suppression_seq] = sim.probe_log_last_sequence();
	int post_suppression_rc = post_suppression_seq_rc == EXIT_SUCCESS ?
		wait_for_observation(admin, sim, post_suppression_seq, cluster, hgs, "post-processing suppression") : EXIT_FAILURE;
	ok(post_suppression_rc == EXIT_SUCCESS && server_has_status(admin, hgs.blue_reader, cluster.blue_readers[0], "ONLINE") &&
		server_absent(admin, hgs.blue_writer, cluster.blue_readers[0]) &&
		no_read_only_log_after(admin, cluster.blue_readers[0], initiated_reader_log),
		"post-processing continues to suppress read-only placement changes for the mapped reader");

	auto [post_repeat_seq_rc, post_repeat_seq] = sim.probe_log_last_sequence();
	rc = post_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_one_reader_pair(cluster, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	int post_repeat_rc = rc == EXIT_SUCCESS ? wait_for_observation(admin, sim, post_repeat_seq, cluster, hgs, "post-processing repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && post_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(admin, sim, post_repeat_seq, hgs, "post-processing repeat", "WRITER_SWITCHOVER_POST_PROCESSING") == EXIT_SUCCESS &&
		server_has_status(admin, hgs.blue_reader, cluster.blue_readers[1], "SHUNNED_AWS_BGD"),
		"repeated post-processing observation preserves status, shun, and placement effects");

	auto [completed_seq_rc, completed_seq] = sim.probe_log_last_sequence();
	rc = completed_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, target_only_completed(cluster)) : EXIT_FAILURE;
	int completed_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, completed_seq, hgs, "target-only completed", "READER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && completed_rc == EXIT_SUCCESS,
		"target-only SWITCHOVER_COMPLETED observation enters inferred reader-switchover status");

	ok(server_has_status(admin, hgs.blue_reader, cluster.blue_readers[1], "SHUNNED_AWS_BGD"),
		"writer completion defers unmatched-reader cleanup until the reader signal");

	auto [completed_repeat_seq_rc, completed_repeat_seq] = sim.probe_log_last_sequence();
	rc = completed_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, target_only_completed(cluster)) : EXIT_FAILURE;
	int completed_repeat_rc = rc == EXIT_SUCCESS ? wait_for_observation(admin, sim, completed_repeat_seq, cluster, hgs, "target-only completed repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && completed_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(admin, sim, completed_repeat_seq, hgs, "target-only completed repeat", "READER_SWITCHOVER_IN_PROGRESS") == EXIT_SUCCESS &&
		server_has_status(admin, hgs.blue_reader, cluster.blue_readers[1], "SHUNNED_AWS_BGD"),
		"repeated target-only completed observation preserves reader-switchover effects");

	auto [empty_seq_rc, empty_seq] = sim.probe_log_last_sequence();
	rc = empty_seq_rc == EXIT_SUCCESS ? sim.topology_delete(backends) : EXIT_FAILURE;
	int empty_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, empty_seq, hgs, "present empty", "NONE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && empty_rc == EXIT_SUCCESS,
		"present-but-empty topology completes reader cleanup and reaches NONE");

	ok(server_has_status(admin, hgs.blue_reader, cluster.blue_readers[1], "ONLINE"),
		"final cleanup unshuns the recorded unmatched blue reader");

	auto [empty_green_rc, empty_green_probe] = bgd_wait_for_probe(sim, empty_seq, cluster.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin, kScenario, "present-empty green observation",
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	const uint64_t blue_probe_baseline = empty_green_rc == EXIT_SUCCESS ? empty_green_probe.sequence_id : empty_seq;
	auto [blue_after_empty_rc, blue_after_empty_probe] = bgd_wait_for_probe(sim, blue_probe_baseline,
		cluster.blue_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin, kScenario,
		"post-cleanup blue probe", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	ok(empty_green_rc == EXIT_SUCCESS && blue_after_empty_rc == EXIT_SUCCESS &&
		empty_green_probe.sequence_id < blue_after_empty_probe.sequence_id,
		"final cleanup removes the direct green probe pin and resumes blue-IP metadata probing");

	auto [final_green_writer_pool_rc, final_green_writer_pool] = bgd_connection_pool_count(admin, hgs.green_writer);
	auto [final_green_reader_pool_rc, final_green_reader_pool] = bgd_connection_pool_count(admin, hgs.green_reader);
	ok(final_green_writer_pool_rc == EXIT_SUCCESS && final_green_writer_pool == 0 &&
		final_green_reader_pool_rc == EXIT_SUCCESS && final_green_reader_pool == 0,
		"final cleanup drains eligible green hostgroup pools");

	ok(green_rows_remain_online(admin, hgs, cluster),
		"final cleanup retains all configured green rows and statuses");

	if (scenario_cleanup(admin, sim, backends) != EXIT_SUCCESS) {
		diag("failed to clean normal lifecycle scenario");
	}
	mysql_close(admin);
	return exit_status();
}
