/**
 * @file test_rds_bgd_rollback-t.cpp
 * @brief Accepted AWS RDS Blue/Green cancellation and rollback coverage.
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

vector<Endpoint> topology_backends(RDS_BGD_Cluster& cluster) {
	return { cluster.blue_writer.endpoint(), cluster.green_writer.endpoint() };
}

vector<RDS_BGD_Topology_Row> topology_with_reader_pair(RDS_BGD_Cluster& cluster, const string& status) {
	vector<RDS_BGD_Topology_Row> rows = cluster.get_topology(status);
	rows.push_back({ cluster.blue_readers[0].hostname, cluster.blue_readers[0].hostname,
		cluster.blue_readers[0].port, "BLUE_GREEN_DEPLOYMENT_SOURCE", status });
	rows.push_back({ cluster.green_readers[0].hostname, cluster.green_readers[0].hostname,
		cluster.green_readers[0].port, "BLUE_GREEN_DEPLOYMENT_TARGET", status });
	return rows;
}

int reset_scenario(MYSQL* admin, RDS_BGD_Simulator& sim, const vector<Endpoint>& backends) {
	return bgd_admin_cleanup(admin) == EXIT_SUCCESS && sim.topology_drop(backends) == EXIT_SUCCESS ?
		EXIT_SUCCESS : EXIT_FAILURE;
}

void set_read_only(RDS_BGD_Simulator& sim, RDS_BGD_Host& host, bool value) {
	if (sim.read_only_update(host.host_endpoint(), value) != EXIT_SUCCESS) BAIL_OUT("failed to configure simulated read_only state");
}

int wait_for_status(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence, const string& scenario,
	const BGD_Hostgroups& hgs, const string& phase, const string& status)
{
	return bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status=" + bgd_sql_quote(status),
		kTimeoutSeconds, sim, sequence, scenario, phase, "runtime BGD status " + status,
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
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

int wait_for_placement(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence, const string& scenario,
	const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster, const string& phase, bool writer_demoted)
{
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=" +
		string(writer_demoted ? "0" : "1") + " AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=" +
		string(writer_demoted ? "1" : "0") + " AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[0].hostname) + " AND port=3306 AND status='ONLINE')=1 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[1].hostname) + " AND port=3306 AND status='ONLINE')=1";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, phase,
		writer_demoted ? "blue writer demotion" : "blue writer and readers restored", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

int set_default_hostgroup(MYSQL* admin, int hostgroup) {
	return execute_all(admin, { "UPDATE mysql_users SET default_hostgroup=" + to_string(hostgroup) +
		" WHERE username='testuser'", "LOAD MYSQL USERS TO RUNTIME" });
}

rc_t<string> connect_and_echo(const CommandLine& cl) {
	MYSQL* client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	if (client == nullptr) return { EXIT_FAILURE, {} };
	auto result = bgd_backend_ip_echo(client);
	mysql_close(client);
	return result;
}

int64_t read_only_log_time(MYSQL* admin, const RDS_BGD_Host& host) {
	auto [rc, rows] = mysql_query_ext_rows(admin, "SELECT COALESCE(MAX(time_start_us),0) FROM mysql_server_read_only_log WHERE hostname=" +
		bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 ? strtoll(rows[0][0].c_str(), nullptr, 10) : -1;
}

int wait_for_read_only_log(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence, const string& scenario,
	const BGD_Hostgroups& hgs, const RDS_BGD_Host& host, int64_t baseline)
{
	return bgd_wait_for_condition(admin, "SELECT COUNT(*)>0 FROM mysql_server_read_only_log WHERE hostname=" +
		bgd_sql_quote(host.hostname) + " AND port=3306 AND time_start_us>" + to_string(baseline),
		kTimeoutSeconds, sim, sequence, scenario, "post-rollback read_only", "new read_only telemetry", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

rc_t<vector<mysql_res_row>> green_row_snapshot(MYSQL* admin, const string& table, int hostgroup, const RDS_BGD_Host& host) {
	return mysql_query_ext_rows(admin, "SELECT hostgroup_id,hostname,port,status,use_ssl,weight,max_connections FROM " + table +
		" WHERE hostgroup_id=" + to_string(hostgroup) + " AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
}

bool snapshot_unchanged(MYSQL* admin, const string& table, int hostgroup, const RDS_BGD_Host& host,
	const vector<mysql_res_row>& expected)
{
	auto [rc, rows] = green_row_snapshot(admin, table, hostgroup, host);
	return rc == EXIT_SUCCESS && rows == expected;
}

bool green_snapshots_unchanged(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster,
	const vector<mysql_res_row>& admin_writer, const vector<mysql_res_row>& runtime_writer,
	const vector<mysql_res_row>& admin_reader = {}, const vector<mysql_res_row>& runtime_reader = {})
{
	return snapshot_unchanged(admin, "mysql_servers", hgs.green_writer, cluster.green_writer, admin_writer) &&
		snapshot_unchanged(admin, "runtime_mysql_servers", hgs.green_writer, cluster.green_writer, runtime_writer) &&
		(admin_reader.empty() || snapshot_unchanged(admin, "mysql_servers", hgs.green_reader, cluster.green_readers[0], admin_reader)) &&
		(runtime_reader.empty() || snapshot_unchanged(admin, "runtime_mysql_servers", hgs.green_reader, cluster.green_readers[0], runtime_reader));
}

} // namespace

int main() {
	plan(13);
	CommandLine cl {};
	if (cl.getEnv()) BAIL_OUT("failed to load TAP environment");
	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) BAIL_OUT("failed to connect to ProxySQL Admin");
	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin); BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}
	if (bgd_register_test_cleanup(admin, sim) != EXIT_SUCCESS) BAIL_OUT("failed to register BGD TAP cleanup");

	// Accepted initiated cancellation: monitor creates this target server in an initially empty configured green HG.
	RDS_BGD_Cluster created = bgd_cluster_init();
	BGD_Hostgroups created_hgs { 980, 981, 982, 983 };
	vector<Endpoint> created_backends = topology_backends(created);
	if (reset_scenario(admin, sim, created_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset initiated rollback scenario");
	set_read_only(sim, created.blue_writer, false);
	set_read_only(sim, created.green_writer, false);
	set_read_only(sim, created.blue_readers[0], true);
	set_read_only(sim, created.blue_readers[1], true);
	auto [created_available_rc, created_available_seq] = sim.probe_log_last_sequence();
	int rc = created_available_rc == EXIT_SUCCESS ? sim.topology_update(created_backends, topology_with_reader_pair(created, "AVAILABLE")) : EXIT_FAILURE;
	int available_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, created, created_hgs, BGD_Admin_Mode::explicit_configuration,
		{ created.blue_writer, created.blue_readers[0], created.blue_readers[1] }) : EXIT_FAILURE;
	available_rc = available_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, created_available_seq, "initiated-cancel", created_hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	int created_green_rc = available_rc == EXIT_SUCCESS ? bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_servers WHERE hostgroup_id=982 AND hostname=" + bgd_sql_quote(created.green_writer.hostname) + " AND port=3306",
		kTimeoutSeconds, sim, created_available_seq, "initiated-cancel", "available", "monitor-created green writer runtime row", created_hgs.blue_writer,
		{ created_hgs.blue_writer, created_hgs.blue_reader, created_hgs.green_writer, created_hgs.green_reader }) : EXIT_FAILURE;
	auto [created_admin_rc, created_admin] = green_row_snapshot(admin, "mysql_servers", created_hgs.green_writer, created.green_writer);
	auto [created_runtime_rc, created_runtime] = green_row_snapshot(admin, "runtime_mysql_servers", created_hgs.green_writer, created.green_writer);
	ok(rc == EXIT_SUCCESS && available_rc == EXIT_SUCCESS && created_green_rc == EXIT_SUCCESS && created_admin_rc == EXIT_SUCCESS &&
		created_runtime_rc == EXIT_SUCCESS && created_admin.empty() && created_runtime.size() == 1,
		"AVAILABLE monitor creates the exact target green writer in runtime while Admin remains unchanged");

	auto [initiated_seq_rc, initiated_seq] = sim.probe_log_last_sequence();
	rc = initiated_seq_rc == EXIT_SUCCESS ? sim.topology_update(created_backends, topology_with_reader_pair(created, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	int initiated_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, initiated_seq, "initiated-cancel", created_hgs, "initiated", "WRITER_SWITCHOVER_INITIATED") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_rc == EXIT_SUCCESS, "recorded SWITCHOVER_INITIATED enters the accepted cancellation phase");

	int64_t created_read_only_baseline = read_only_log_time(admin, created.blue_readers[0]);
	auto [return_seq_rc, return_seq] = sim.probe_log_last_sequence();
	rc = return_seq_rc == EXIT_SUCCESS ? sim.topology_update(created_backends, topology_with_reader_pair(created, "AVAILABLE")) : EXIT_FAILURE;
	int returned_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, return_seq, "initiated-cancel", created_hgs, "returned available", "AVAILABLE") : EXIT_FAILURE;
	int restored_rc = returned_rc == EXIT_SUCCESS ? wait_for_placement(admin, sim, return_seq, "initiated-cancel", created_hgs, created, "rollback placement", false) : EXIT_FAILURE;
	auto [created_probe_rc, created_probe] = bgd_wait_for_probe(sim, return_seq, created.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata,
		kProbeTimeoutMs, 0, admin, "initiated-cancel", "returned available probe", created_hgs.blue_writer,
		{ created_hgs.blue_writer, created_hgs.blue_reader, created_hgs.green_writer, created_hgs.green_reader });
	ok(rc == EXIT_SUCCESS && returned_rc == EXIT_SUCCESS && restored_rc == EXIT_SUCCESS && created_probe_rc == EXIT_SUCCESS &&
		green_snapshots_unchanged(admin, created_hgs, created, created_admin, created_runtime),
		"initiated cancellation restores full blue placement and retains the monitor-created green writer row");

	set_read_only(sim, created.blue_readers[0], false);
	int unsuppressed_rc = created_read_only_baseline >= 0 ? wait_for_read_only_log(admin, sim, return_seq, "initiated-cancel", created_hgs,
		created.blue_readers[0], created_read_only_baseline) : EXIT_FAILURE;
	ok(unsuppressed_rc == EXIT_SUCCESS, "initiated rollback clears read_only suppression for a discriminating reader action");

	auto [created_repeat_seq_rc, created_repeat_seq] = sim.probe_log_last_sequence();
	rc = created_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(created_backends, topology_with_reader_pair(created, "AVAILABLE")) : EXIT_FAILURE;
	int created_repeat_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, created_repeat_seq, "initiated-cancel", created_hgs, "available repeat", "AVAILABLE") : EXIT_FAILURE;
	int created_repeat_placement_rc = created_repeat_rc == EXIT_SUCCESS ? wait_for_placement(admin, sim, created_repeat_seq, "initiated-cancel", created_hgs, created, "repeat placement", false) : EXIT_FAILURE;
	auto [created_repeat_probe_rc, created_repeat_probe] = bgd_wait_for_probe(sim, created_repeat_seq, created.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata,
		kProbeTimeoutMs, 0, admin, "initiated-cancel", "available repeat probe", created_hgs.blue_writer,
		{ created_hgs.blue_writer, created_hgs.blue_reader, created_hgs.green_writer, created_hgs.green_reader });
	ok(rc == EXIT_SUCCESS && created_repeat_rc == EXIT_SUCCESS && created_repeat_placement_rc == EXIT_SUCCESS && created_repeat_probe_rc == EXIT_SUCCESS &&
		green_snapshots_unchanged(admin, created_hgs, created, created_admin, created_runtime),
		"repeated returned AVAILABLE preserves full placement, probe target, and monitor-created green row");

	// Accepted cancellation from SWITCHOVER_IN_PROGRESS: explicit green membership and its pools are not rollback-owned.
	RDS_BGD_Cluster explicit_cluster = bgd_cluster_2_init();
	BGD_Hostgroups explicit_hgs { 990, 991, 992, 993 };
	vector<Endpoint> explicit_backends = topology_backends(explicit_cluster);
	if (reset_scenario(admin, sim, explicit_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset in-progress rollback scenario");
	set_read_only(sim, explicit_cluster.blue_writer, false);
	set_read_only(sim, explicit_cluster.green_writer, false);
	set_read_only(sim, explicit_cluster.blue_readers[0], true);
	set_read_only(sim, explicit_cluster.blue_readers[1], true);
	auto [explicit_available_seq_rc, explicit_available_seq] = sim.probe_log_last_sequence();
	rc = explicit_available_seq_rc == EXIT_SUCCESS ? sim.topology_update(explicit_backends, topology_with_reader_pair(explicit_cluster, "AVAILABLE")) : EXIT_FAILURE;
	int explicit_setup_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, explicit_cluster, explicit_hgs, BGD_Admin_Mode::explicit_configuration,
		{ explicit_cluster.blue_writer, explicit_cluster.blue_readers[0], explicit_cluster.blue_readers[1] },
		{ explicit_cluster.green_writer, explicit_cluster.green_readers[0] }) : EXIT_FAILURE;
	int explicit_available_rc = explicit_setup_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, explicit_available_seq, "in-progress-cancel", explicit_hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	auto [explicit_admin_writer_rc, explicit_admin_writer] = green_row_snapshot(admin, "mysql_servers", explicit_hgs.green_writer, explicit_cluster.green_writer);
	auto [explicit_runtime_writer_rc, explicit_runtime_writer] = green_row_snapshot(admin, "runtime_mysql_servers", explicit_hgs.green_writer, explicit_cluster.green_writer);
	auto [explicit_admin_reader_rc, explicit_admin_reader] = green_row_snapshot(admin, "mysql_servers", explicit_hgs.green_reader, explicit_cluster.green_readers[0]);
	auto [explicit_runtime_reader_rc, explicit_runtime_reader] = green_row_snapshot(admin, "runtime_mysql_servers", explicit_hgs.green_reader, explicit_cluster.green_readers[0]);
	ok(rc == EXIT_SUCCESS && explicit_available_rc == EXIT_SUCCESS && explicit_admin_writer_rc == EXIT_SUCCESS && explicit_runtime_writer_rc == EXIT_SUCCESS &&
		explicit_admin_reader_rc == EXIT_SUCCESS && explicit_runtime_reader_rc == EXIT_SUCCESS && explicit_admin_writer.size() == 1 &&
		explicit_runtime_writer.size() == 1 && explicit_admin_reader.size() == 1 && explicit_runtime_reader.size() == 1,
		"explicit configuration snapshots pre-existing green writer and reader Admin and runtime rows");

	int green_pool_setup_rc = set_default_hostgroup(admin, explicit_hgs.green_writer) == EXIT_SUCCESS ? connect_and_echo(cl).first : EXIT_FAILURE;
	green_pool_setup_rc = green_pool_setup_rc == EXIT_SUCCESS && set_default_hostgroup(admin, explicit_hgs.green_reader) == EXIT_SUCCESS ? connect_and_echo(cl).first : EXIT_FAILURE;
	green_pool_setup_rc = green_pool_setup_rc == EXIT_SUCCESS ? set_default_hostgroup(admin, explicit_hgs.blue_writer) : EXIT_FAILURE;
	auto [green_writer_pool_rc, green_writer_pool] = bgd_connection_pool_count(admin, explicit_hgs.green_writer);
	auto [green_reader_pool_rc, green_reader_pool] = bgd_connection_pool_count(admin, explicit_hgs.green_reader);
	ok(green_pool_setup_rc == EXIT_SUCCESS && green_writer_pool_rc == EXIT_SUCCESS && green_writer_pool >= 1 &&
		green_reader_pool_rc == EXIT_SUCCESS && green_reader_pool >= 1, "green pools exist before in-progress cancellation");

	auto [explicit_initiated_seq_rc, explicit_initiated_seq] = sim.probe_log_last_sequence();
	rc = explicit_initiated_seq_rc == EXIT_SUCCESS ? sim.topology_update(explicit_backends, topology_with_reader_pair(explicit_cluster, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	int explicit_initiated_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, explicit_initiated_seq, "in-progress-cancel", explicit_hgs, "initiated", "WRITER_SWITCHOVER_INITIATED") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && explicit_initiated_rc == EXIT_SUCCESS, "in-progress scenario first observes SWITCHOVER_INITIATED");

	auto [progress_seq_rc, progress_seq] = sim.probe_log_last_sequence();
	rc = progress_seq_rc == EXIT_SUCCESS ? sim.topology_update(explicit_backends, topology_with_reader_pair(explicit_cluster, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	int progress_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, progress_seq, "in-progress-cancel", explicit_hgs, "in progress", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	int demoted_rc = progress_rc == EXIT_SUCCESS ? wait_for_placement(admin, sim, progress_seq, "in-progress-cancel", explicit_hgs, explicit_cluster, "writer demotion", true) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && progress_rc == EXIT_SUCCESS && demoted_rc == EXIT_SUCCESS, "in-progress cancellation is entered only after blue writer demotion");

	auto [explicit_return_seq_rc, explicit_return_seq] = sim.probe_log_last_sequence();
	rc = explicit_return_seq_rc == EXIT_SUCCESS ? sim.topology_update(explicit_backends, topology_with_reader_pair(explicit_cluster, "AVAILABLE")) : EXIT_FAILURE;
	int explicit_returned_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, explicit_return_seq, "in-progress-cancel", explicit_hgs, "returned available", "AVAILABLE") : EXIT_FAILURE;
	int explicit_restored_rc = explicit_returned_rc == EXIT_SUCCESS ? wait_for_placement(admin, sim, explicit_return_seq, "in-progress-cancel", explicit_hgs, explicit_cluster, "rollback placement", false) : EXIT_FAILURE;
	auto [available_probe_rc, available_probe] = bgd_wait_for_probe(sim, explicit_return_seq, explicit_cluster.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin, "in-progress-cancel", "returned available probe", explicit_hgs.blue_writer,
		{ explicit_hgs.blue_writer, explicit_hgs.blue_reader, explicit_hgs.green_writer, explicit_hgs.green_reader });
	ok(rc == EXIT_SUCCESS && explicit_returned_rc == EXIT_SUCCESS && explicit_restored_rc == EXIT_SUCCESS && available_probe_rc == EXIT_SUCCESS,
		"in-progress rollback restores full blue placement and rebuilds the AVAILABLE direct probe target");

	auto [post_writer_pool_rc, post_writer_pool] = bgd_connection_pool_count(admin, explicit_hgs.green_writer);
	auto [post_reader_pool_rc, post_reader_pool] = bgd_connection_pool_count(admin, explicit_hgs.green_reader);
	ok(post_writer_pool_rc == EXIT_SUCCESS && post_writer_pool >= green_writer_pool && post_reader_pool_rc == EXIT_SUCCESS &&
		post_reader_pool >= green_reader_pool && green_snapshots_unchanged(admin, explicit_hgs, explicit_cluster,
			explicit_admin_writer, explicit_runtime_writer, explicit_admin_reader, explicit_runtime_reader),
		"rollback leaves explicit green rows and their pre-existing pools unchanged");

	auto [blue_echo_rc, blue_echo] = connect_and_echo(cl);
	ok(blue_echo_rc == EXIT_SUCCESS && blue_echo.find(explicit_cluster.blue_writer.ip) != string::npos,
		"returned AVAILABLE routes a new blue-writer connection to the blue backend IP");

	auto [explicit_repeat_seq_rc, explicit_repeat_seq] = sim.probe_log_last_sequence();
	rc = explicit_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(explicit_backends, topology_with_reader_pair(explicit_cluster, "AVAILABLE")) : EXIT_FAILURE;
	int explicit_repeat_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, explicit_repeat_seq, "in-progress-cancel", explicit_hgs, "available repeat", "AVAILABLE") : EXIT_FAILURE;
	int repeat_placement_rc = explicit_repeat_rc == EXIT_SUCCESS ? wait_for_placement(admin, sim, explicit_repeat_seq, "in-progress-cancel", explicit_hgs, explicit_cluster, "repeat placement", false) : EXIT_FAILURE;
	auto [repeat_probe_rc, repeat_probe] = bgd_wait_for_probe(sim, explicit_repeat_seq, explicit_cluster.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin, "in-progress-cancel", "available repeat probe", explicit_hgs.blue_writer,
		{ explicit_hgs.blue_writer, explicit_hgs.blue_reader, explicit_hgs.green_writer, explicit_hgs.green_reader });
	auto [repeat_writer_pool_rc, repeat_writer_pool] = bgd_connection_pool_count(admin, explicit_hgs.green_writer);
	auto [repeat_reader_pool_rc, repeat_reader_pool] = bgd_connection_pool_count(admin, explicit_hgs.green_reader);
	ok(rc == EXIT_SUCCESS && explicit_repeat_rc == EXIT_SUCCESS && repeat_placement_rc == EXIT_SUCCESS && repeat_probe_rc == EXIT_SUCCESS &&
		repeat_writer_pool_rc == EXIT_SUCCESS && repeat_writer_pool >= green_writer_pool && repeat_reader_pool_rc == EXIT_SUCCESS &&
		repeat_reader_pool >= green_reader_pool && green_snapshots_unchanged(admin, explicit_hgs, explicit_cluster,
			explicit_admin_writer, explicit_runtime_writer, explicit_admin_reader, explicit_runtime_reader),
		"repeated returned AVAILABLE preserves placement, probe routing, green rows, and green pools");

	int cleanup_rc = bgd_finish_test_cleanup(admin, sim);
	if (cleanup_rc != EXIT_SUCCESS) BAIL_OUT("failed to clean final rollback TAP state");
	mysql_close(admin);
	return exit_status();
}
