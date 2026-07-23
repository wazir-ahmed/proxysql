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

bool blue_placement_restored(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster) {
	return server_has_status(admin, hgs.blue_writer, cluster.blue_writer, "ONLINE") &&
		server_absent(admin, hgs.blue_reader, cluster.blue_writer) &&
		server_has_status(admin, hgs.blue_reader, cluster.blue_readers[0], "ONLINE") &&
		server_absent(admin, hgs.blue_writer, cluster.blue_readers[0]) &&
		server_has_status(admin, hgs.blue_reader, cluster.blue_readers[1], "ONLINE");
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
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[0].hostname) + " AND port=3306 AND status='ONLINE')=1";
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

bool automatic_row_unchanged(MYSQL* admin, const BGD_Hostgroups& hgs) {
	auto [rc, rows] = bgd_runtime_rows(admin, hgs.blue_writer);
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 6 && rows[0][2].empty() && rows[0][3].empty() &&
		rows[0][4] == "1" && rows[0][5] == "AVAILABLE";
}

bool explicit_green_rows_unchanged(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster) {
	return server_has_status(admin, hgs.green_writer, cluster.green_writer, "ONLINE") &&
		server_has_status(admin, hgs.green_reader, cluster.green_readers[0], "ONLINE");
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

	// Accepted cancellation from SWITCHOVER_INITIATED: automatic runtime configuration remains owned by discovery.
	RDS_BGD_Cluster automatic = bgd_cluster_init();
	BGD_Hostgroups automatic_hgs { 980, 981, 982, 983 };
	vector<Endpoint> automatic_backends = topology_backends(automatic);
	if (reset_scenario(admin, sim, automatic_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset initiated rollback scenario");
	set_read_only(sim, automatic.blue_writer, false);
	set_read_only(sim, automatic.green_writer, false);
	set_read_only(sim, automatic.blue_readers[0], true);
	set_read_only(sim, automatic.blue_readers[1], true);
	auto [automatic_available_rc, automatic_available_seq] = sim.probe_log_last_sequence();
	int rc = automatic_available_rc == EXIT_SUCCESS ? sim.topology_update(automatic_backends, topology_with_reader_pair(automatic, "AVAILABLE")) : EXIT_FAILURE;
	int available_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, automatic, automatic_hgs, BGD_Admin_Mode::automatic,
		{ automatic.blue_writer, automatic.blue_readers[0], automatic.blue_readers[1] }) : EXIT_FAILURE;
	available_rc = available_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, automatic_available_seq, "initiated-cancel", automatic_hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && available_rc == EXIT_SUCCESS && automatic_row_unchanged(admin, automatic_hgs),
		"automatic discovery records an AVAILABLE row with nullable green hostgroups");

	auto [initiated_seq_rc, initiated_seq] = sim.probe_log_last_sequence();
	rc = initiated_seq_rc == EXIT_SUCCESS ? sim.topology_update(automatic_backends, topology_with_reader_pair(automatic, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	int initiated_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, initiated_seq, "initiated-cancel", automatic_hgs, "initiated", "WRITER_SWITCHOVER_INITIATED") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_rc == EXIT_SUCCESS, "recorded SWITCHOVER_INITIATED enters the accepted cancellation phase");

	int64_t automatic_read_only_baseline = read_only_log_time(admin, automatic.blue_readers[0]);
	auto [return_seq_rc, return_seq] = sim.probe_log_last_sequence();
	rc = return_seq_rc == EXIT_SUCCESS ? sim.topology_update(automatic_backends, topology_with_reader_pair(automatic, "AVAILABLE")) : EXIT_FAILURE;
	int returned_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, return_seq, "initiated-cancel", automatic_hgs, "returned available", "AVAILABLE") : EXIT_FAILURE;
	int restored_rc = returned_rc == EXIT_SUCCESS ? wait_for_placement(admin, sim, return_seq, "initiated-cancel", automatic_hgs, automatic, "rollback placement", false) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && returned_rc == EXIT_SUCCESS && restored_rc == EXIT_SUCCESS && automatic_row_unchanged(admin, automatic_hgs),
		"initiated cancellation returns AVAILABLE and retains the auto-added runtime row and blue placement");

	set_read_only(sim, automatic.blue_readers[0], false);
	int unsuppressed_rc = automatic_read_only_baseline >= 0 ? wait_for_read_only_log(admin, sim, return_seq, "initiated-cancel", automatic_hgs,
		automatic.blue_readers[0], automatic_read_only_baseline) : EXIT_FAILURE;
	ok(unsuppressed_rc == EXIT_SUCCESS, "initiated rollback clears read_only suppression for a discriminating reader action");

	auto [automatic_repeat_seq_rc, automatic_repeat_seq] = sim.probe_log_last_sequence();
	rc = automatic_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(automatic_backends, topology_with_reader_pair(automatic, "AVAILABLE")) : EXIT_FAILURE;
	int automatic_repeat_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, automatic_repeat_seq, "initiated-cancel", automatic_hgs, "available repeat", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && automatic_repeat_rc == EXIT_SUCCESS && automatic_row_unchanged(admin, automatic_hgs),
		"repeated returned AVAILABLE leaves automatic rollback effects stable");

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
	ok(rc == EXIT_SUCCESS && explicit_available_rc == EXIT_SUCCESS && explicit_green_rows_unchanged(admin, explicit_hgs, explicit_cluster),
		"explicit configuration reaches AVAILABLE with retained green rows");

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
		"in-progress rollback restores blue writer and reader placement and rebuilds the AVAILABLE direct probe target");

	auto [post_writer_pool_rc, post_writer_pool] = bgd_connection_pool_count(admin, explicit_hgs.green_writer);
	auto [post_reader_pool_rc, post_reader_pool] = bgd_connection_pool_count(admin, explicit_hgs.green_reader);
	ok(post_writer_pool_rc == EXIT_SUCCESS && post_writer_pool >= green_writer_pool && post_reader_pool_rc == EXIT_SUCCESS &&
		post_reader_pool >= green_reader_pool && explicit_green_rows_unchanged(admin, explicit_hgs, explicit_cluster),
		"rollback leaves explicit green rows ONLINE and does not drain their pre-existing pools");

	auto [blue_echo_rc, blue_echo] = connect_and_echo(cl);
	ok(blue_echo_rc == EXIT_SUCCESS && blue_echo.find(explicit_cluster.blue_writer.ip) != string::npos,
		"in-progress rollback removes temporary blue-to-green routing pins");

	auto [explicit_repeat_seq_rc, explicit_repeat_seq] = sim.probe_log_last_sequence();
	rc = explicit_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(explicit_backends, topology_with_reader_pair(explicit_cluster, "AVAILABLE")) : EXIT_FAILURE;
	int explicit_repeat_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, explicit_repeat_seq, "in-progress-cancel", explicit_hgs, "available repeat", "AVAILABLE") : EXIT_FAILURE;
	int repeat_placement_rc = explicit_repeat_rc == EXIT_SUCCESS ? wait_for_placement(admin, sim, explicit_repeat_seq, "in-progress-cancel", explicit_hgs, explicit_cluster, "repeat placement", false) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && explicit_repeat_rc == EXIT_SUCCESS && repeat_placement_rc == EXIT_SUCCESS && explicit_green_rows_unchanged(admin, explicit_hgs, explicit_cluster),
		"repeated returned AVAILABLE preserves rollback placement and explicit green membership");

	if (reset_scenario(admin, sim, explicit_backends) != EXIT_SUCCESS) diag("failed to clean rollback scenario");
	mysql_close(admin);
	return exit_status();
}
