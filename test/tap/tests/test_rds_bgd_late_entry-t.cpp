/**
 * @file test_rds_bgd_late_entry-t.cpp
 * @brief Fresh-worker AWS RDS Blue/Green late-entry characterization coverage.
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

vector<Endpoint> topology_backends(RDS_BGD_Cluster& cluster) {
	return { cluster.blue_writer.endpoint(), cluster.green_writer.endpoint() };
}

vector<RDS_BGD_Topology_Row> topology_with_reader_pair(RDS_BGD_Cluster& cluster, const string& status) {
	vector<RDS_BGD_Topology_Row> rows = cluster.get_topology(status);
	rows.push_back({ cluster.blue_readers[0].hostname, cluster.blue_readers[0].hostname, 3306,
		"BLUE_GREEN_DEPLOYMENT_SOURCE", status });
	rows.push_back({ cluster.green_readers[0].hostname, cluster.green_readers[0].hostname, 3306,
		"BLUE_GREEN_DEPLOYMENT_TARGET", status });
	return rows;
}

vector<RDS_BGD_Topology_Row> target_only_completed(RDS_BGD_Cluster& cluster) {
	return {{ cluster.green_writer.hostname, cluster.green_writer.hostname, 3306,
		"BLUE_GREEN_DEPLOYMENT_TARGET", "SWITCHOVER_COMPLETED" }};
}

int reset_scenario(MYSQL* admin, RDS_BGD_Simulator& sim, const vector<Endpoint>& backends) {
	return bgd_admin_cleanup(admin) == EXIT_SUCCESS && sim.topology_drop(backends) == EXIT_SUCCESS ?
		EXIT_SUCCESS : EXIT_FAILURE;
}

void set_read_only(RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster, bool writer, bool reader) {
	if (sim.read_only_update(cluster.blue_writer.host_endpoint(), writer) != EXIT_SUCCESS ||
		sim.read_only_update(cluster.green_writer.host_endpoint(), false) != EXIT_SUCCESS ||
		sim.read_only_update(cluster.blue_readers[0].host_endpoint(), reader) != EXIT_SUCCESS ||
		sim.read_only_update(cluster.blue_readers[1].host_endpoint(), true) != EXIT_SUCCESS ||
		sim.read_only_update(cluster.green_readers[0].host_endpoint(), true) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure simulated read_only state");
	}
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

int wait_for_green_metadata(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence, const string& scenario,
	RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs, const string& phase)
{
	return bgd_wait_for_probe(sim, sequence, cluster.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata,
		kProbeTimeoutMs, 0, admin, scenario, phase, hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader }).first;
}

int wait_for_blue_metadata(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence, const string& scenario,
	RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs, const string& phase)
{
	return bgd_wait_for_probe(sim, sequence, cluster.blue_writer.endpoint(), RDS_BGD_Probe_Kind::metadata,
		kProbeTimeoutMs, 0, admin, scenario, phase, hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader }).first;
}

bool server_has_status(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, const string& status) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT status FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == status;
}

bool server_absent(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

bool writer_placement(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster,
	bool in_writer, bool in_reader)
{
	return (in_writer ? server_has_status(admin, hgs.blue_writer, cluster.blue_writer, "ONLINE")
		: server_absent(admin, hgs.blue_writer, cluster.blue_writer)) &&
		(in_reader ? server_has_status(admin, hgs.blue_reader, cluster.blue_writer, "ONLINE")
		: server_absent(admin, hgs.blue_reader, cluster.blue_writer));
}

int wait_for_in_progress_placement(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster, const string& phase)
{
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=0 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[0].hostname) + " AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[0].hostname) + " AND port=3306)=0";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, phase,
		"blue writer demoted to reader placement with mapped reader retained", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

int64_t last_read_only_log_time(MYSQL* admin, const RDS_BGD_Host& host) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COALESCE(MAX(time_start_us),0) FROM mysql_server_read_only_log WHERE hostname=" +
		bgd_sql_quote(host.hostname) + " AND port=3306");
	if (rc != EXIT_SUCCESS || rows.size() != 1 || rows[0].size() != 1) return -1;
	return strtoll(rows[0][0].c_str(), nullptr, 10);
}

bool no_read_only_log_after(MYSQL* admin, const RDS_BGD_Host& host, int64_t baseline) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM mysql_server_read_only_log WHERE hostname=" + bgd_sql_quote(host.hostname) +
		" AND port=3306 AND time_start_us>" + to_string(baseline));
	return baseline >= 0 && rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

int wait_for_precompletion_rollback(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster)
{
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=0 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[0].hostname) + " AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[1].hostname) + " AND port=3306 AND status='ONLINE')=1";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, "present-empty",
		"rollback restores blue placement and reader availability", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

rc_t<string> connect_and_echo(const CommandLine& cl) {
	MYSQL* client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	if (client == nullptr) return { EXIT_FAILURE, {} };
	auto result = bgd_backend_ip_echo(client);
	mysql_close(client);
	return result;
}

int set_default_hostgroup(MYSQL* admin, int hostgroup) {
	return execute_all(admin, {
		"UPDATE mysql_users SET default_hostgroup=" + to_string(hostgroup) + " WHERE username='testuser'",
		"LOAD MYSQL USERS TO RUNTIME",
	});
}

int configure_servers_without_bgd_worker(MYSQL* admin, const RDS_BGD_Cluster& cluster,
	const BGD_Hostgroups& hgs)
{
	vector<string> queries {
		"INSERT INTO mysql_replication_hostgroups(writer_hostgroup,reader_hostgroup) VALUES (" +
			to_string(hgs.blue_writer) + "," + to_string(hgs.blue_reader) + ")",
		"SET mysql-monitor_username='testuser'",
		"SET mysql-monitor_password='testuser'",
		"SET mysql-monitor_enabled='true'",
		"SET mysql-monitor_read_only_interval=100",
		"SET mysql-monitor_aws_rds_topology_discovery_interval=1",
		"SET mysql-aws_blue_green_deployment_auto_discovery='false'",
		"UPDATE mysql_users SET default_hostgroup=" + to_string(hgs.blue_writer) + " WHERE username='testuser'",
	};
	return execute_all(admin, queries) == EXIT_SUCCESS &&
		bgd_admin_add_servers(admin, cluster, hgs,
			{ cluster.blue_writer, cluster.blue_readers[0], cluster.blue_readers[1] }, false, 0) == EXIT_SUCCESS &&
		bgd_admin_add_servers(admin, cluster, hgs,
			{ cluster.green_writer, cluster.green_readers[0] }, true, 0) == EXIT_SUCCESS &&
		execute_all(admin, { "LOAD MYSQL VARIABLES TO RUNTIME", "LOAD MYSQL USERS TO RUNTIME", "LOAD MYSQL SERVERS TO RUNTIME" }) == EXIT_SUCCESS ?
		EXIT_SUCCESS : EXIT_FAILURE;
}

int enable_explicit_bgd_worker(MYSQL* admin, const BGD_Hostgroups& hgs) {
	return execute_all(admin, {
		"INSERT INTO mysql_aws_rds_bgd_hostgroups("
		"writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup,"
		"active,writer_is_also_reader,check_interval_ms,check_timeout_ms,comment) VALUES (" +
		to_string(hgs.blue_writer) + "," + to_string(hgs.blue_reader) + "," + to_string(hgs.green_writer) + "," +
		to_string(hgs.green_reader) + ",1,0,100,800,'late-entry post pool baseline')",
		"LOAD MYSQL SERVERS TO RUNTIME",
	});
}

int establish_green_pools(const CommandLine& cl, MYSQL* admin, const BGD_Hostgroups& hgs) {
	int rc = set_default_hostgroup(admin, hgs.green_writer);
	if (rc == EXIT_SUCCESS) rc = connect_and_echo(cl).first;
	if (rc == EXIT_SUCCESS) rc = set_default_hostgroup(admin, hgs.green_reader);
	if (rc == EXIT_SUCCESS) rc = connect_and_echo(cl).first;
	if (set_default_hostgroup(admin, hgs.blue_writer) != EXIT_SUCCESS) rc = EXIT_FAILURE;
	return rc;
}

bool green_pools_are_drained(MYSQL* admin, const BGD_Hostgroups& hgs) {
	auto [writer_rc, writer] = bgd_connection_pool_count(admin, hgs.green_writer);
	auto [reader_rc, reader] = bgd_connection_pool_count(admin, hgs.green_reader);
	return writer_rc == EXIT_SUCCESS && writer == 0 && reader_rc == EXIT_SUCCESS && reader == 0;
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
	if (bgd_register_test_cleanup(admin, sim) != EXIT_SUCCESS) BAIL_OUT("failed to register BGD TAP cleanup");

	// A fresh worker first sees SWITCHOVER_INITIATED.
	RDS_BGD_Cluster initiated = bgd_cluster_init();
	BGD_Hostgroups initiated_hgs { 1170, 1171, 1172, 1173 };
	vector<Endpoint> initiated_backends = topology_backends(initiated);
	if (reset_scenario(admin, sim, initiated_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset initiated late-entry scenario");
	set_read_only(sim, initiated, false, true);
	auto [initiated_publish_seq_rc, initiated_publish_seq] = sim.probe_log_last_sequence();
	int rc = initiated_publish_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(initiated_backends, topology_with_reader_pair(initiated, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	int initiated_setup_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, initiated, initiated_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ initiated.blue_writer, initiated.blue_readers[0], initiated.blue_readers[1] },
		{ initiated.green_writer, initiated.green_readers[0] }) : EXIT_FAILURE;
	int initiated_status_rc = initiated_setup_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, initiated_publish_seq,
		"fresh-initiated", initiated_hgs, "first observation", "WRITER_SWITCHOVER_INITIATED") : EXIT_FAILURE;
	int initiated_probe_rc = initiated_status_rc == EXIT_SUCCESS ? wait_for_green_metadata(admin, sim, initiated_publish_seq,
		"fresh-initiated", initiated, initiated_hgs, "first observation") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_setup_rc == EXIT_SUCCESS && initiated_status_rc == EXIT_SUCCESS && initiated_probe_rc == EXIT_SUCCESS &&
		writer_placement(admin, initiated_hgs, initiated, true, false),
		"fresh initiated observation builds prerequisites and enters initiated without writer demotion");

	int64_t initiated_writer_log = last_read_only_log_time(admin, initiated.blue_writer);
	int64_t initiated_reader_log = last_read_only_log_time(admin, initiated.blue_readers[0]);
	set_read_only(sim, initiated, true, false);
	auto [initiated_suppression_seq_rc, initiated_suppression_seq] = sim.probe_log_last_sequence();
	int initiated_suppression_rc = initiated_suppression_seq_rc == EXIT_SUCCESS ? wait_for_green_metadata(admin, sim,
		initiated_suppression_seq, "fresh-initiated", initiated, initiated_hgs, "suppression") : EXIT_FAILURE;
	ok(initiated_suppression_rc == EXIT_SUCCESS && writer_placement(admin, initiated_hgs, initiated, true, false) &&
		server_has_status(admin, initiated_hgs.blue_reader, initiated.blue_readers[0], "ONLINE") &&
		no_read_only_log_after(admin, initiated.blue_writer, initiated_writer_log) &&
		no_read_only_log_after(admin, initiated.blue_readers[0], initiated_reader_log),
		"fresh initiated observation enables read-only suppression for deployment members");

	set_read_only(sim, initiated, false, true);
	auto [initiated_repeat_seq_rc, initiated_repeat_seq] = sim.probe_log_last_sequence();
	rc = initiated_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(initiated_backends,
		topology_with_reader_pair(initiated, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	int initiated_repeat_rc = rc == EXIT_SUCCESS ? wait_for_green_metadata(admin, sim, initiated_repeat_seq,
		"fresh-initiated", initiated, initiated_hgs, "repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(admin, sim, initiated_repeat_seq, "fresh-initiated", initiated_hgs, "repeat", "WRITER_SWITCHOVER_INITIATED") == EXIT_SUCCESS &&
		writer_placement(admin, initiated_hgs, initiated, true, false),
		"repeated fresh initiated observation preserves initiated status and placement");

	auto [initiated_empty_seq_rc, initiated_empty_seq] = sim.probe_log_last_sequence();
	rc = initiated_empty_seq_rc == EXIT_SUCCESS ? sim.topology_delete(initiated_backends) : EXIT_FAILURE;
	int initiated_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, initiated_empty_seq,
		"fresh-initiated", initiated_hgs, "present-empty", "NONE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_none_rc == EXIT_SUCCESS &&
		wait_for_precompletion_rollback(admin, sim, initiated_empty_seq, "fresh-initiated", initiated_hgs, initiated) == EXIT_SUCCESS,
		"fresh initiated present-empty cleanup safely clears suppression and restores baseline placement");

	// A fresh worker first sees SWITCHOVER_IN_PROGRESS.
	RDS_BGD_Cluster progress = bgd_cluster_2_init();
	BGD_Hostgroups progress_hgs { 1180, 1181, 1182, 1183 };
	vector<Endpoint> progress_backends = topology_backends(progress);
	if (reset_scenario(admin, sim, progress_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset in-progress late-entry scenario");
	set_read_only(sim, progress, false, true);
	auto [progress_publish_seq_rc, progress_publish_seq] = sim.probe_log_last_sequence();
	rc = progress_publish_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(progress_backends, topology_with_reader_pair(progress, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	int progress_setup_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, progress, progress_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ progress.blue_writer, progress.blue_readers[0], progress.blue_readers[1] },
		{ progress.green_writer, progress.green_readers[0] }) : EXIT_FAILURE;
	int progress_status_rc = progress_setup_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, progress_publish_seq,
		"fresh-in-progress", progress_hgs, "first observation", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	int progress_probe_rc = progress_status_rc == EXIT_SUCCESS ? wait_for_green_metadata(admin, sim, progress_publish_seq,
		"fresh-in-progress", progress, progress_hgs, "first observation") : EXIT_FAILURE;
	int progress_effects_rc = progress_probe_rc == EXIT_SUCCESS ? wait_for_in_progress_placement(admin, sim, progress_publish_seq,
		"fresh-in-progress", progress_hgs, progress, "first observation effects") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && progress_setup_rc == EXIT_SUCCESS && progress_status_rc == EXIT_SUCCESS && progress_probe_rc == EXIT_SUCCESS &&
		progress_effects_rc == EXIT_SUCCESS,
		"fresh in-progress observation builds prerequisites before demoting the blue writer");

	int64_t progress_reader_log = last_read_only_log_time(admin, progress.blue_readers[0]);
	set_read_only(sim, progress, true, false);
	auto [progress_suppression_seq_rc, progress_suppression_seq] = sim.probe_log_last_sequence();
	int progress_suppression_rc = progress_suppression_seq_rc == EXIT_SUCCESS ? wait_for_green_metadata(admin, sim,
		progress_suppression_seq, "fresh-in-progress", progress, progress_hgs, "suppression") : EXIT_FAILURE;
	ok(progress_suppression_rc == EXIT_SUCCESS && writer_placement(admin, progress_hgs, progress, false, true) &&
		server_has_status(admin, progress_hgs.blue_reader, progress.blue_readers[0], "ONLINE") &&
		no_read_only_log_after(admin, progress.blue_readers[0], progress_reader_log),
		"fresh in-progress observation suppresses read-only placement changes after demotion");

	set_read_only(sim, progress, false, true);
	auto [progress_repeat_seq_rc, progress_repeat_seq] = sim.probe_log_last_sequence();
	rc = progress_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(progress_backends,
		topology_with_reader_pair(progress, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	int progress_repeat_rc = rc == EXIT_SUCCESS ? wait_for_green_metadata(admin, sim, progress_repeat_seq,
		"fresh-in-progress", progress, progress_hgs, "repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && progress_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(admin, sim, progress_repeat_seq, "fresh-in-progress", progress_hgs, "repeat", "WRITER_SWITCHOVER_IN_PROGRESS") == EXIT_SUCCESS &&
		wait_for_in_progress_placement(admin, sim, progress_repeat_seq, "fresh-in-progress", progress_hgs, progress, "repeat effects") == EXIT_SUCCESS,
		"repeated fresh in-progress observation preserves the direct-entry demotion");

	auto [progress_empty_seq_rc, progress_empty_seq] = sim.probe_log_last_sequence();
	rc = progress_empty_seq_rc == EXIT_SUCCESS ? sim.topology_delete(progress_backends) : EXIT_FAILURE;
	int progress_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, progress_empty_seq,
		"fresh-in-progress", progress_hgs, "present-empty", "NONE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && progress_none_rc == EXIT_SUCCESS &&
		wait_for_precompletion_rollback(admin, sim, progress_empty_seq, "fresh-in-progress", progress_hgs, progress) == EXIT_SUCCESS,
		"fresh in-progress present-empty cleanup restores the demoted blue writer");

	// A fresh worker first sees SWITCHOVER_IN_POST_PROCESSING.  Create a causal blue pool
	// only after publishing topology but before the BGD row can start its worker.
	RDS_BGD_Cluster post = bgd_cluster_3_init();
	BGD_Hostgroups post_hgs { 1190, 1191, 1192, 1193 };
	vector<Endpoint> post_backends = topology_backends(post);
	if (reset_scenario(admin, sim, post_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset post-processing late-entry scenario");
	set_read_only(sim, post, false, true);
	auto [post_publish_seq_rc, post_publish_seq] = sim.probe_log_last_sequence();
	rc = post_publish_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(post_backends, topology_with_reader_pair(post, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	int post_servers_rc = rc == EXIT_SUCCESS ? configure_servers_without_bgd_worker(admin, post, post_hgs) : EXIT_FAILURE;
	auto [post_pool_connect_rc, post_pool_echo] = post_servers_rc == EXIT_SUCCESS ? connect_and_echo(cl) : rc_t<string> { EXIT_FAILURE, {} };
	auto [post_pool_before_rc, post_pool_before] = bgd_connection_pool_count(admin, post_hgs.blue_writer, post.blue_writer.hostname);
	int post_worker_rc = post_pool_connect_rc == EXIT_SUCCESS && post_pool_before_rc == EXIT_SUCCESS && post_pool_before >= 1 ?
		enable_explicit_bgd_worker(admin, post_hgs) : EXIT_FAILURE;
	int post_status_rc = post_worker_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, post_publish_seq,
		"fresh-post-processing", post_hgs, "first observation", "WRITER_SWITCHOVER_POST_PROCESSING") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && post_servers_rc == EXIT_SUCCESS && post_pool_connect_rc == EXIT_SUCCESS &&
		post_pool_echo.find(post.blue_writer.ip) != string::npos && post_pool_before_rc == EXIT_SUCCESS && post_pool_before >= 1 &&
		post_worker_rc == EXIT_SUCCESS && post_status_rc == EXIT_SUCCESS,
		"fresh post-processing topology is published before worker creation with a causal blue pool baseline");

	int post_effects_rc = post_status_rc == EXIT_SUCCESS ? bgd_wait_for_condition(admin,
		"SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(post_hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(post.blue_writer.hostname) + " AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(post_hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(post.blue_writer.hostname) + " AND port=3306)=0 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(post_hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(post.blue_readers[0].hostname) + " AND port=3306 AND status='ONLINE')=1",
		kTimeoutSeconds, sim, post_publish_seq, "fresh-post-processing", "first observation",
		"writer placement and mapped-reader availability", post_hgs.blue_writer,
		{ post_hgs.blue_writer, post_hgs.blue_reader, post_hgs.green_writer, post_hgs.green_reader }) : EXIT_FAILURE;
	int post_pool_drain_rc = post_effects_rc == EXIT_SUCCESS ? bgd_wait_for_condition(admin,
		"SELECT COALESCE(SUM(ConnUsed+ConnFree),0)=0 FROM stats_mysql_connection_pool WHERE srv_host=" +
		bgd_sql_quote(post.blue_writer.hostname), kTimeoutSeconds, sim, post_publish_seq, "fresh-post-processing", "blue drain",
		"causal blue writer pool drained", post_hgs.blue_writer,
		{ post_hgs.blue_writer, post_hgs.blue_reader, post_hgs.green_writer, post_hgs.green_reader }) : EXIT_FAILURE;
	auto [post_echo_rc, post_echo] = post_pool_drain_rc == EXIT_SUCCESS ? connect_and_echo(cl) : rc_t<string> { EXIT_FAILURE, {} };
	ok(post_effects_rc == EXIT_SUCCESS && post_pool_drain_rc == EXIT_SUCCESS && post_echo_rc == EXIT_SUCCESS &&
		post_echo.find(post.green_writer.ip) != string::npos,
		"fresh post-processing builds map and resolution before pinning, draining, and placement");

	int64_t post_reader_log = last_read_only_log_time(admin, post.blue_readers[0]);
	set_read_only(sim, post, true, false);
	auto [post_suppression_seq_rc, post_suppression_seq] = sim.probe_log_last_sequence();
	int post_suppression_rc = post_suppression_seq_rc == EXIT_SUCCESS ? wait_for_green_metadata(admin, sim,
		post_suppression_seq, "fresh-post-processing", post, post_hgs, "suppression") : EXIT_FAILURE;
	ok(post_suppression_rc == EXIT_SUCCESS && server_has_status(admin, post_hgs.blue_reader, post.blue_readers[0], "ONLINE") &&
		no_read_only_log_after(admin, post.blue_readers[0], post_reader_log),
		"fresh post-processing retains read-only suppression while mapped readers remain placed");

	set_read_only(sim, post, false, true);
	auto [post_repeat_seq_rc, post_repeat_seq] = sim.probe_log_last_sequence();
	rc = post_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(post_backends,
		topology_with_reader_pair(post, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	int post_repeat_rc = rc == EXIT_SUCCESS ? wait_for_green_metadata(admin, sim, post_repeat_seq,
		"fresh-post-processing", post, post_hgs, "repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && post_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(admin, sim, post_repeat_seq, "fresh-post-processing", post_hgs, "repeat", "WRITER_SWITCHOVER_POST_PROCESSING") == EXIT_SUCCESS &&
		writer_placement(admin, post_hgs, post, true, false) &&
		server_has_status(admin, post_hgs.blue_reader, post.blue_readers[0], "ONLINE"),
		"repeated fresh post-processing observation preserves writer and mapped-reader placement");

	auto [post_empty_seq_rc, post_empty_seq] = sim.probe_log_last_sequence();
	rc = post_empty_seq_rc == EXIT_SUCCESS ? sim.topology_delete(post_backends) : EXIT_FAILURE;
	int post_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, post_empty_seq,
		"fresh-post-processing", post_hgs, "present-empty", "NONE") : EXIT_FAILURE;
	int post_blue_probe_rc = post_none_rc == EXIT_SUCCESS ? wait_for_blue_metadata(admin, sim, post_empty_seq,
		"fresh-post-processing", post, post_hgs, "post-cleanup blue probe") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && post_none_rc == EXIT_SUCCESS &&
		wait_for_precompletion_rollback(admin, sim, post_empty_seq, "fresh-post-processing", post_hgs, post) == EXIT_SUCCESS &&
		post_blue_probe_rc == EXIT_SUCCESS,
		"fresh post-processing present-empty rollback restores readers and removes the temporary blue pin");

	// A fresh worker first sees the target-only completed observation.  It has no prior map
	// or effects to reconstruct, but reader-phase cleanup still drains configured green pools.
	RDS_BGD_Cluster completed = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups completed_hgs { 1200, 1201, 1202, 1203 };
	vector<Endpoint> completed_backends = topology_backends(completed);
	if (reset_scenario(admin, sim, completed_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset completed late-entry scenario");
	set_read_only(sim, completed, false, true);
	auto [completed_publish_seq_rc, completed_publish_seq] = sim.probe_log_last_sequence();
	rc = completed_publish_seq_rc == EXIT_SUCCESS ? sim.topology_update(completed_backends,
		target_only_completed(completed)) : EXIT_FAILURE;
	int completed_setup_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, completed, completed_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ completed.blue_writer, completed.blue_readers[0], completed.blue_readers[1] },
		{ completed.green_writer, completed.green_readers[0] }) : EXIT_FAILURE;
	int completed_status_rc = completed_setup_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, completed_publish_seq,
		"fresh-completed", completed_hgs, "first observation", "READER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	int completed_blue_probe_rc = completed_status_rc == EXIT_SUCCESS ? wait_for_blue_metadata(admin, sim, completed_publish_seq,
		"fresh-completed", completed, completed_hgs, "first observation") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && completed_setup_rc == EXIT_SUCCESS && completed_status_rc == EXIT_SUCCESS && completed_blue_probe_rc == EXIT_SUCCESS,
		"fresh target-only completed observation enters inferred reader-switchover status");

	auto [completed_no_green_rc, completed_no_green] = bgd_wait_for_probe_from_backends(sim, completed_publish_seq,
		{ completed.green_writer.endpoint() }, RDS_BGD_Probe_Kind::metadata, kNoProbeTimeoutMs, 0);
	if (completed_no_green_rc != ETIMEDOUT) {
		diag("fresh-completed: green metadata negative check returned rc=%d", completed_no_green_rc);
	}
	auto [completed_echo_rc, completed_echo] = completed_no_green_rc == ETIMEDOUT ?
		connect_and_echo(cl) : rc_t<string> { EXIT_FAILURE, {} };
	ok(completed_no_green_rc == ETIMEDOUT && completed_echo_rc == EXIT_SUCCESS &&
		completed_echo.find(completed.blue_writer.ip) != string::npos && writer_placement(admin, completed_hgs, completed, true, false) &&
		server_has_status(admin, completed_hgs.blue_reader, completed.blue_readers[1], "ONLINE"),
		"fresh completed does not reconstruct earlier direct-green map, blue pin, or demotion effects");

	auto [completed_repeat_seq_rc, completed_repeat_seq] = sim.probe_log_last_sequence();
	rc = completed_repeat_seq_rc == EXIT_SUCCESS ? sim.topology_update(completed_backends,
		target_only_completed(completed)) : EXIT_FAILURE;
	int completed_repeat_rc = rc == EXIT_SUCCESS ? wait_for_blue_metadata(admin, sim, completed_repeat_seq,
		"fresh-completed", completed, completed_hgs, "repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && completed_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(admin, sim, completed_repeat_seq, "fresh-completed", completed_hgs, "repeat", "READER_SWITCHOVER_IN_PROGRESS") == EXIT_SUCCESS &&
		writer_placement(admin, completed_hgs, completed, true, false),
		"repeated fresh completed observation remains in reader-switchover without reconstructed effects");

	int completed_pools_rc = establish_green_pools(cl, admin, completed_hgs);
	auto [completed_writer_pool_rc, completed_writer_pool] = bgd_connection_pool_count(admin, completed_hgs.green_writer);
	auto [completed_reader_pool_rc, completed_reader_pool] = bgd_connection_pool_count(admin, completed_hgs.green_reader);
	ok(completed_pools_rc == EXIT_SUCCESS && completed_writer_pool_rc == EXIT_SUCCESS && completed_writer_pool >= 1 &&
		completed_reader_pool_rc == EXIT_SUCCESS && completed_reader_pool >= 1,
		"fresh completed reader phase accepts green pool baselines before terminal cleanup");

	auto [completed_empty_seq_rc, completed_empty_seq] = sim.probe_log_last_sequence();
	rc = completed_empty_seq_rc == EXIT_SUCCESS ? sim.topology_delete(completed_backends) : EXIT_FAILURE;
	int completed_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, completed_empty_seq,
		"fresh-completed", completed_hgs, "present-empty", "NONE") : EXIT_FAILURE;
	int completed_drain_rc = completed_none_rc == EXIT_SUCCESS ? bgd_wait_for_condition(admin,
		"SELECT "
		"(SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(completed_hgs.green_writer) + ")=0 AND "
		"(SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(completed_hgs.green_reader) + ")=0",
		kTimeoutSeconds, sim, completed_empty_seq, "fresh-completed", "present-empty",
		"configured green pools drained", completed_hgs.blue_writer,
		{ completed_hgs.blue_writer, completed_hgs.blue_reader, completed_hgs.green_writer, completed_hgs.green_reader }) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && completed_none_rc == EXIT_SUCCESS && completed_drain_rc == EXIT_SUCCESS &&
		green_pools_are_drained(admin, completed_hgs) && writer_placement(admin, completed_hgs, completed, true, false) &&
		server_has_status(admin, completed_hgs.blue_reader, completed.blue_readers[1], "ONLINE"),
		"fresh completed present-empty cleanup reaches NONE safely and drains configured green pools");

	int cleanup_rc = bgd_finish_test_cleanup(admin, sim);
	if (cleanup_rc != EXIT_SUCCESS) BAIL_OUT("failed to clean final late-entry TAP state");
	mysql_close(admin);
	return exit_status();
}
