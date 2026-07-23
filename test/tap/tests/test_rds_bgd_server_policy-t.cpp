/**
 * @file test_rds_bgd_server_policy-t.cpp
 * @brief AWS RDS Blue/Green server eligibility, persistence, and drain policy coverage.
 */

#include <cstdlib>
#include <string>
#include <vector>

#include "command_line.h"
#include "rds_bgd_tap.h"
#include "utils.h"

namespace {

const uint32_t kTimeoutSeconds = 3;

struct Green_Server {
	int hostgroup;
	RDS_BGD_Host host;
	string status;
};

vector<Endpoint> scenario_backends(RDS_BGD_Cluster& cluster) {
	vector<Endpoint> backends { cluster.blue_writer.endpoint(), cluster.green_writer.endpoint() };
	for (RDS_BGD_Host& host : cluster.blue_readers) backends.push_back(host.endpoint());
	for (RDS_BGD_Host& host : cluster.green_readers) backends.push_back(host.endpoint());
	return backends;
}

vector<RDS_BGD_Topology_Row> topology_with_reader_pairs(RDS_BGD_Cluster& cluster, const string& status,
	size_t pairs)
{
	vector<RDS_BGD_Topology_Row> rows = cluster.get_topology(status);
	for (size_t i = 0; i < pairs; ++i) {
		rows.push_back({ cluster.blue_readers[i].hostname, cluster.blue_readers[i].hostname, 3306,
			"BLUE_GREEN_DEPLOYMENT_SOURCE", status });
		rows.push_back({ cluster.green_readers[i].hostname, cluster.green_readers[i].hostname, 3306,
			"BLUE_GREEN_DEPLOYMENT_TARGET", status });
	}
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

void set_writers_writable(RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster) {
	if (sim.read_only_update(cluster.blue_writer.host_endpoint(), false) != EXIT_SUCCESS ||
		sim.read_only_update(cluster.green_writer.host_endpoint(), false) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure simulated writer read_only state");
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

bool server_has_status(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, const string& status) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT status FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == status;
}

bool server_present(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, const string& status = "") {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT status FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 &&
		(status.empty() || rows[0][0] == status);
}

bool persistent_server_has_status(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, const string& status) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT status FROM mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == status;
}

int set_default_hostgroup(MYSQL* admin, int hostgroup) {
	return execute_all(admin, {
		"UPDATE mysql_users SET default_hostgroup=" + to_string(hostgroup) + " WHERE username='testuser'",
		"LOAD MYSQL USERS TO RUNTIME",
	});
}

int create_pool(const CommandLine& cl, MYSQL* admin, int hostgroup) {
	if (set_default_hostgroup(admin, hostgroup) != EXIT_SUCCESS) return EXIT_FAILURE;
	MYSQL* client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	if (client == nullptr) return EXIT_FAILURE;
	auto [echo_rc, echo] = bgd_backend_ip_echo(client);
	mysql_close(client);
	return echo_rc == EXIT_SUCCESS ? EXIT_SUCCESS : EXIT_FAILURE;
}

int restore_blue_default_hostgroup(MYSQL* admin, const BGD_Hostgroups& hgs) {
	return set_default_hostgroup(admin, hgs.blue_writer);
}

rc_t<int64_t> pool_for_hostname(MYSQL* admin, const string& hostname) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE srv_host=" +
		bgd_sql_quote(hostname));
	if (rc != EXIT_SUCCESS || rows.size() != 1 || rows[0].size() != 1) return { EXIT_FAILURE, 0 };
	return { EXIT_SUCCESS, strtoll(rows[0][0].c_str(), nullptr, 10) };
}

rc_t<vector<mysql_res_row>> green_snapshot(MYSQL* admin, const string& table, const BGD_Hostgroups& hgs) {
	return mysql_query_ext_rows(admin,
		"SELECT hostgroup_id,hostname,port,status,use_ssl,weight,max_connections FROM " + table +
		" WHERE hostgroup_id IN (" + to_string(hgs.green_writer) + "," + to_string(hgs.green_reader) +
		") ORDER BY hostgroup_id,hostname,port");
}

bool snapshots_unchanged(MYSQL* admin, const BGD_Hostgroups& hgs,
	const vector<mysql_res_row>& admin_snapshot, const vector<mysql_res_row>& runtime_snapshot)
{
	auto [admin_rc, current_admin] = green_snapshot(admin, "mysql_servers", hgs);
	auto [runtime_rc, current_runtime] = green_snapshot(admin, "runtime_mysql_servers", hgs);
	return admin_rc == EXIT_SUCCESS && runtime_rc == EXIT_SUCCESS &&
		current_admin == admin_snapshot && current_runtime == runtime_snapshot;
}

int add_green_server(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, const string& status) {
	return execute_all(admin, {
		"INSERT INTO mysql_servers(hostgroup_id,hostname,port,status,use_ssl,comment) VALUES (" +
		to_string(hostgroup) + "," + bgd_sql_quote(host.hostname) + ",3306," + bgd_sql_quote(status) +
		",0," + bgd_sql_quote("BGD TAP green " + host.ip) + ")",
	});
}

int set_server_status(MYSQL* admin, const Green_Server& server, const string& status) {
	return execute_all(admin, {
		"UPDATE mysql_servers SET status=" + bgd_sql_quote(status) + " WHERE hostgroup_id=" +
		to_string(server.hostgroup) + " AND hostname=" + bgd_sql_quote(server.host.hostname) + " AND port=3306",
	});
}

int load_servers(MYSQL* admin) {
	return execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" });
}

bool configured_green_statuses_match(MYSQL* admin, const vector<Green_Server>& servers) {
	for (const Green_Server& server : servers) {
		if (server.status == "OFFLINE_HARD") {
			if (!persistent_server_has_status(admin, server.hostgroup, server.host, server.status) ||
				server_present(admin, server.hostgroup, server.host)) return false;
		} else if (!server_has_status(admin, server.hostgroup, server.host, server.status)) {
			return false;
		}
	}
	return true;
}

}  // namespace

int main() {
	plan(13);

	CommandLine cl {};
	if (cl.getEnv()) BAIL_OUT("failed to load TAP environment");
	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) BAIL_OUT("failed to connect to ProxySQL Admin");
	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}

	// Eligible reader matching is independent from the unmatched-reader policy.  Rollback must
	// leave every configured green row and its pre-existing pool untouched.
	RDS_BGD_Cluster matched = bgd_cluster_init();
	BGD_Hostgroups matched_hgs { 1280, 1281, 1282, 1283 };
	vector<Endpoint> matched_backends = scenario_backends(matched);
	if (reset_scenario(admin, sim, matched_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset matched-reader scenario");
	set_writers_writable(sim, matched);
	auto [matched_available_seq_rc, matched_available_seq] = sim.probe_log_last_sequence();
	int rc = matched_available_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(matched_backends, topology_with_reader_pairs(matched, "AVAILABLE", 1)) : EXIT_FAILURE;
	int matched_setup_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, matched, matched_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ matched.blue_writer, matched.blue_readers[0], matched.blue_readers[1] },
		{ matched.green_writer, matched.green_readers[0] }) : EXIT_FAILURE;
	int matched_available_rc = matched_setup_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, matched_available_seq,
		"matched-unmatched", matched_hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && matched_available_rc == EXIT_SUCCESS,
		"matched-unmatched: recorded AVAILABLE reaches the configured BGD worker");

	int matched_pool_rc = create_pool(cl, admin, matched_hgs.green_writer);
	matched_pool_rc = matched_pool_rc == EXIT_SUCCESS ? create_pool(cl, admin, matched_hgs.green_reader) : EXIT_FAILURE;
	matched_pool_rc = matched_pool_rc == EXIT_SUCCESS ? restore_blue_default_hostgroup(admin, matched_hgs) : EXIT_FAILURE;
	auto [matched_writer_pool_rc, matched_writer_pool] = bgd_connection_pool_count(admin, matched_hgs.green_writer);
	auto [matched_reader_pool_rc, matched_reader_pool] = bgd_connection_pool_count(admin, matched_hgs.green_reader);
	auto [matched_admin_snapshot_rc, matched_admin_snapshot] = green_snapshot(admin, "mysql_servers", matched_hgs);
	auto [matched_runtime_snapshot_rc, matched_runtime_snapshot] = green_snapshot(admin, "runtime_mysql_servers", matched_hgs);
	if (matched_pool_rc != EXIT_SUCCESS || matched_writer_pool_rc != EXIT_SUCCESS || matched_reader_pool_rc != EXIT_SUCCESS ||
		matched_admin_snapshot_rc != EXIT_SUCCESS || matched_runtime_snapshot_rc != EXIT_SUCCESS) {
		BAIL_OUT("failed to create matched-reader rollback baselines");
	}

	auto [matched_post_seq_rc, matched_post_seq] = sim.probe_log_last_sequence();
	rc = matched_post_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(matched_backends, topology_with_reader_pairs(matched, "SWITCHOVER_IN_POST_PROCESSING", 1)) : EXIT_FAILURE;
	int matched_post_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, matched_post_seq,
		"matched-unmatched", matched_hgs, "post processing", "WRITER_SWITCHOVER_POST_PROCESSING") : EXIT_FAILURE;
	int matched_effects_rc = matched_post_rc == EXIT_SUCCESS ? bgd_wait_for_condition(admin,
		"SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=1281 AND hostname=" +
		bgd_sql_quote(matched.blue_readers[0].hostname) + " AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=1281 AND hostname=" +
		bgd_sql_quote(matched.blue_readers[1].hostname) + " AND port=3306 AND status='SHUNNED_AWS_BGD')=1",
		kTimeoutSeconds, sim, matched_post_seq, "matched-unmatched", "post processing",
		"eligible mapped reader remains ONLINE and only unmatched reader is BGD shunned", matched_hgs.blue_writer,
		{ matched_hgs.blue_writer, matched_hgs.blue_reader, matched_hgs.green_writer, matched_hgs.green_reader }) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && matched_effects_rc == EXIT_SUCCESS,
		"matched-unmatched: eligible pair stays mapped while only the unmatched blue reader is SHUNNED_AWS_BGD");

	auto [matched_rollback_seq_rc, matched_rollback_seq] = sim.probe_log_last_sequence();
	rc = matched_rollback_seq_rc == EXIT_SUCCESS ? sim.topology_delete(matched_backends) : EXIT_FAILURE;
	int matched_rollback_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, matched_rollback_seq,
		"matched-unmatched", matched_hgs, "pre-completion empty topology", "NONE") : EXIT_FAILURE;
	auto [matched_writer_after_rc, matched_writer_after] = bgd_connection_pool_count(admin, matched_hgs.green_writer);
	auto [matched_reader_after_rc, matched_reader_after] = bgd_connection_pool_count(admin, matched_hgs.green_reader);
	ok(rc == EXIT_SUCCESS && matched_rollback_rc == EXIT_SUCCESS && matched_writer_after_rc == EXIT_SUCCESS &&
		matched_reader_after_rc == EXIT_SUCCESS && matched_writer_after >= matched_writer_pool &&
		matched_reader_after >= matched_reader_pool && snapshots_unchanged(admin, matched_hgs,
			matched_admin_snapshot, matched_runtime_snapshot),
		"matched-unmatched: rollback preserves exact green rows and does not drain their established pools");

	// Offline blue rows are excluded from both map construction and reader shunning.  With the
	// remaining eligible reader unmatched because its green counterpart is OFFLINE_HARD, the
	// monitor keeps the writer temporarily in the reader hostgroup.
	RDS_BGD_Cluster fallback = bgd_cluster_2_init();
	RDS_BGD_Cluster fallback_extra = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups fallback_hgs { 1290, 1291, 1292, 1293 };
	vector<Endpoint> fallback_backends = scenario_backends(fallback);
	fallback_backends.push_back(fallback_extra.blue_readers[0].endpoint());
	if (reset_scenario(admin, sim, fallback_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset offline fallback scenario");
	set_writers_writable(sim, fallback);
	if (bgd_admin_setup(admin, fallback, fallback_hgs, BGD_Admin_Mode::explicit_configuration,
		{ fallback.blue_writer, fallback.blue_readers[0], fallback.blue_readers[1] },
		{ fallback.green_writer, fallback.green_readers[0] }) != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, fallback, fallback_hgs, { fallback_extra.blue_readers[0] }, false, 0) != EXIT_SUCCESS ||
		execute_all(admin, {
			"UPDATE mysql_servers SET status='OFFLINE_SOFT' WHERE hostgroup_id=1291 AND hostname=" +
				bgd_sql_quote(fallback.blue_readers[1].hostname) + " AND port=3306",
			"UPDATE mysql_servers SET status='OFFLINE_HARD' WHERE hostgroup_id=1291 AND hostname=" +
				bgd_sql_quote(fallback_extra.blue_readers[0].hostname) + " AND port=3306",
			"UPDATE mysql_servers SET status='OFFLINE_HARD' WHERE hostgroup_id=1293 AND hostname=" +
				bgd_sql_quote(fallback.green_readers[0].hostname) + " AND port=3306",
			"LOAD MYSQL SERVERS TO RUNTIME",
		}) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure offline fallback server states");
	}
	auto [fallback_seq_rc, fallback_seq] = sim.probe_log_last_sequence();
	rc = fallback_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(fallback_backends, topology_with_reader_pairs(fallback, "SWITCHOVER_IN_POST_PROCESSING", 1)) : EXIT_FAILURE;
	int fallback_post_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, fallback_seq,
		"offline-fallback", fallback_hgs, "post processing", "WRITER_SWITCHOVER_POST_PROCESSING") : EXIT_FAILURE;
	int fallback_effects_rc = fallback_post_rc == EXIT_SUCCESS ? bgd_wait_for_condition(admin,
		"SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=1291 AND hostname=" +
		bgd_sql_quote(fallback.blue_readers[0].hostname) + " AND port=3306 AND status='SHUNNED_AWS_BGD')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=1291 AND hostname=" +
		bgd_sql_quote(fallback.blue_readers[1].hostname) + " AND port=3306 AND status='OFFLINE_SOFT')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=1291 AND hostname=" +
		bgd_sql_quote(fallback_extra.blue_readers[0].hostname) + " AND port=3306)=0 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=1291 AND hostname=" +
		bgd_sql_quote(fallback.blue_writer.hostname) + " AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=1293 AND hostname=" +
		bgd_sql_quote(fallback.green_readers[0].hostname) + " AND port=3306)=0",
		kTimeoutSeconds, sim, fallback_seq, "offline-fallback", "post processing",
		"only eligible unmatched reader shunned, offline blue rows excluded, writer fallback retained", fallback_hgs.blue_writer,
		{ fallback_hgs.blue_writer, fallback_hgs.blue_reader, fallback_hgs.green_writer, fallback_hgs.green_reader }) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && fallback_effects_rc == EXIT_SUCCESS &&
		persistent_server_has_status(admin, fallback_hgs.blue_reader, fallback_extra.blue_readers[0], "OFFLINE_HARD") &&
		persistent_server_has_status(admin, fallback_hgs.green_reader, fallback.green_readers[0], "OFFLINE_HARD"),
		"offline-fallback: OFFLINE_SOFT/HARD blue rows are untouched and an ineligible green counterpart triggers the accepted writer fallback");

	// The cleanup matrix covers one persistent green row per status.  Each row first receives
	// an isolated pool while ONLINE; the post-status pool values are the causal pre-cleanup
	// baselines used to distinguish BGD cleanup from status-transition side effects.
	RDS_BGD_Cluster matrix = bgd_cluster_3_init();
	RDS_BGD_Cluster matrix_extra = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups matrix_hgs { 1300, 1301, 1302, 1303 };
	vector<Endpoint> matrix_backends = scenario_backends(matrix);
	matrix_backends.push_back(matrix_extra.green_readers[0].endpoint());
	matrix_backends.push_back(matrix_extra.green_readers[1].endpoint());
	if (reset_scenario(admin, sim, matrix_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset green drain matrix scenario");
	set_writers_writable(sim, matrix);
	if (bgd_admin_setup(admin, matrix, matrix_hgs, BGD_Admin_Mode::explicit_configuration,
		{ matrix.blue_writer, matrix.blue_readers[0], matrix.blue_readers[1] },
		{ matrix.green_writer, matrix.green_readers[0], matrix.green_readers[1] }) != EXIT_SUCCESS ||
		add_green_server(admin, matrix_hgs.green_reader, matrix_extra.green_readers[0], "ONLINE") != EXIT_SUCCESS ||
		add_green_server(admin, matrix_hgs.green_reader, matrix_extra.green_readers[1], "ONLINE") != EXIT_SUCCESS ||
		load_servers(admin) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure green drain status matrix");
	}
	vector<Green_Server> matrix_servers {
		{ matrix_hgs.green_writer, matrix.green_writer, "ONLINE" },
		{ matrix_hgs.green_reader, matrix.green_readers[0], "SHUNNED" },
		{ matrix_hgs.green_reader, matrix.green_readers[1], "SHUNNED" },
		{ matrix_hgs.green_reader, matrix_extra.green_readers[0], "OFFLINE_SOFT" },
		{ matrix_hgs.green_reader, matrix_extra.green_readers[1], "OFFLINE_HARD" },
	};
	const int matrix_router_base_hg = 1350;
	for (size_t i = 0; i < matrix_servers.size(); ++i) {
		if (add_green_server(admin, matrix_router_base_hg + static_cast<int>(i), matrix_servers[i].host, "ONLINE") != EXIT_SUCCESS) {
			BAIL_OUT("failed to add green drain matrix pool router row");
		}
	}
	if (load_servers(admin) != EXIT_SUCCESS) BAIL_OUT("failed to load green drain matrix pool router rows");
	auto [matrix_initial_admin_rc, matrix_initial_admin] = green_snapshot(admin, "mysql_servers", matrix_hgs);
	auto [matrix_initial_runtime_rc, matrix_initial_runtime] = green_snapshot(admin, "runtime_mysql_servers", matrix_hgs);
	ok(matrix_initial_admin_rc == EXIT_SUCCESS && matrix_initial_runtime_rc == EXIT_SUCCESS &&
		matrix_initial_admin.size() == matrix_servers.size() && matrix_initial_runtime.size() == matrix_servers.size(),
		"green-drain-matrix: all five configured green rows exist before status-specific pool baselines");

	int matrix_pool_setup_rc = EXIT_SUCCESS;
	for (size_t i = 0; i < matrix_servers.size() && matrix_pool_setup_rc == EXIT_SUCCESS; ++i) {
		matrix_pool_setup_rc = create_pool(cl, admin, matrix_router_base_hg + static_cast<int>(i));
	}
	if (restore_blue_default_hostgroup(admin, matrix_hgs) != EXIT_SUCCESS) matrix_pool_setup_rc = EXIT_FAILURE;
	vector<int64_t> matrix_before_status {};
	for (const Green_Server& server : matrix_servers) {
		auto [pool_rc, pool] = pool_for_hostname(admin, server.host.hostname);
		if (pool_rc != EXIT_SUCCESS) matrix_pool_setup_rc = EXIT_FAILURE;
		matrix_before_status.push_back(pool);
	}
	ok(matrix_pool_setup_rc == EXIT_SUCCESS && matrix_before_status.size() == matrix_servers.size() &&
		matrix_before_status[0] >= 1 && matrix_before_status[1] >= 1 && matrix_before_status[2] >= 1 &&
		matrix_before_status[3] >= 1 && matrix_before_status[4] >= 1,
		"green-drain-matrix: every eventual status has a causal established pool before its status changes");

	int matrix_status_rc = EXIT_SUCCESS;
	for (const Green_Server& server : matrix_servers) {
		if (set_server_status(admin, server, server.status) != EXIT_SUCCESS) matrix_status_rc = EXIT_FAILURE;
	}
	if (matrix_status_rc == EXIT_SUCCESS) matrix_status_rc = load_servers(admin);
	vector<int64_t> matrix_pre_cleanup {};
	for (const Green_Server& server : matrix_servers) {
		auto [pool_rc, pool] = pool_for_hostname(admin, server.host.hostname);
		if (pool_rc != EXIT_SUCCESS) matrix_status_rc = EXIT_FAILURE;
		matrix_pre_cleanup.push_back(pool);
	}
	auto [matrix_admin_snapshot_rc, matrix_admin_snapshot] = green_snapshot(admin, "mysql_servers", matrix_hgs);
	auto [matrix_runtime_snapshot_rc, matrix_runtime_snapshot] = green_snapshot(admin, "runtime_mysql_servers", matrix_hgs);
	ok(matrix_status_rc == EXIT_SUCCESS && configured_green_statuses_match(admin, matrix_servers) &&
		matrix_admin_snapshot_rc == EXIT_SUCCESS && matrix_runtime_snapshot_rc == EXIT_SUCCESS,
		"green-drain-matrix: status-transition pool baselines and exact persistent/runtime row snapshots are recorded before cleanup");

	auto [matrix_available_seq_rc, matrix_available_seq] = sim.probe_log_last_sequence();
	rc = matrix_available_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(matrix_backends, topology_with_reader_pairs(matrix, "AVAILABLE", 2)) : EXIT_FAILURE;
	int matrix_available_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, matrix_available_seq,
		"green-drain-matrix", matrix_hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	auto [matrix_post_seq_rc, matrix_post_seq] = sim.probe_log_last_sequence();
	rc = rc == EXIT_SUCCESS && matrix_post_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(matrix_backends, topology_with_reader_pairs(matrix, "SWITCHOVER_IN_POST_PROCESSING", 2)) : EXIT_FAILURE;
	int matrix_post_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, matrix_post_seq,
		"green-drain-matrix", matrix_hgs, "post processing", "WRITER_SWITCHOVER_POST_PROCESSING") : EXIT_FAILURE;
	auto [matrix_completed_seq_rc, matrix_completed_seq] = sim.probe_log_last_sequence();
	rc = rc == EXIT_SUCCESS && matrix_completed_seq_rc == EXIT_SUCCESS ?
		sim.topology_update(matrix_backends, target_only_completed(matrix)) : EXIT_FAILURE;
	int matrix_reader_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, matrix_completed_seq,
		"green-drain-matrix", matrix_hgs, "writer completed", "READER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(matrix_available_rc == EXIT_SUCCESS && matrix_post_rc == EXIT_SUCCESS && matrix_reader_rc == EXIT_SUCCESS,
		"green-drain-matrix: successful lifecycle reaches deferred reader cleanup after the status baselines are fixed");

	auto [matrix_cleanup_seq_rc, matrix_cleanup_seq] = sim.probe_log_last_sequence();
	rc = matrix_cleanup_seq_rc == EXIT_SUCCESS ? sim.topology_delete(matrix_backends) : EXIT_FAILURE;
	int matrix_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, matrix_cleanup_seq,
		"green-drain-matrix", matrix_hgs, "successful cleanup", "NONE") : EXIT_FAILURE;
	vector<int64_t> matrix_after_cleanup {};
	for (const Green_Server& server : matrix_servers) {
		auto [pool_rc, pool] = pool_for_hostname(admin, server.host.hostname);
		if (pool_rc != EXIT_SUCCESS) matrix_none_rc = EXIT_FAILURE;
		matrix_after_cleanup.push_back(pool);
	}
	ok(rc == EXIT_SUCCESS && matrix_none_rc == EXIT_SUCCESS && matrix_after_cleanup.size() == matrix_servers.size() &&
		matrix_after_cleanup[0] == 0 && matrix_after_cleanup[1] == 0 && matrix_after_cleanup[2] == 0,
		"green-drain-matrix: successful cleanup drains ONLINE, SHUNNED, and SHUNNED_AWS_BGD green pools");
	ok(matrix_none_rc == EXIT_SUCCESS && matrix_pre_cleanup.size() == matrix_servers.size() &&
		matrix_after_cleanup[3] == matrix_pre_cleanup[3] && matrix_after_cleanup[4] == matrix_pre_cleanup[4],
		"green-drain-matrix: successful cleanup leaves OFFLINE_SOFT and OFFLINE_HARD green pool baselines untouched");
	ok(matrix_none_rc == EXIT_SUCCESS && snapshots_unchanged(admin, matrix_hgs,
		matrix_admin_snapshot, matrix_runtime_snapshot),
		"green-drain-matrix: successful cleanup retains every green row and its original status exactly");

	// Automatic discovery must defer to an administrator-owned explicit BGD row and green row.
	RDS_BGD_Cluster automatic = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups automatic_hgs { 1310, 1311, 1312, 1313 };
	vector<Endpoint> automatic_backends = scenario_backends(automatic);
	if (reset_scenario(admin, sim, automatic_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset automatic ownership scenario");
	set_writers_writable(sim, automatic);
	if (bgd_admin_setup(admin, automatic, automatic_hgs, BGD_Admin_Mode::explicit_configuration,
		{ automatic.blue_writer, automatic.blue_readers[0] }, { automatic.green_writer }) != EXIT_SUCCESS ||
		execute_all(admin, {
			"UPDATE mysql_servers SET status='SHUNNED' WHERE hostgroup_id=1312 AND hostname=" +
				bgd_sql_quote(automatic.green_writer.hostname) + " AND port=3306",
			"SET mysql-aws_blue_green_deployment_auto_discovery='true'",
			"LOAD MYSQL VARIABLES TO RUNTIME",
			"LOAD MYSQL SERVERS TO RUNTIME",
		}) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure automatic ownership scenario");
	}
	auto [automatic_admin_snapshot_rc, automatic_admin_snapshot] = green_snapshot(admin, "mysql_servers", automatic_hgs);
	auto [automatic_runtime_snapshot_rc, automatic_runtime_snapshot] = green_snapshot(admin, "runtime_mysql_servers", automatic_hgs);
	auto [automatic_bgd_snapshot_rc, automatic_bgd_snapshot] = mysql_query_ext_rows(admin,
		"SELECT writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup,active,writer_is_also_reader,check_interval_ms,check_timeout_ms,comment FROM mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=1310");
	auto [automatic_seq_rc, automatic_seq] = sim.probe_log_last_sequence();
	rc = automatic_seq_rc == EXIT_SUCCESS ? sim.topology_update(automatic_backends, automatic.get_topology("AVAILABLE")) : EXIT_FAILURE;
	int automatic_available_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, automatic_seq,
		"automatic-ownership", automatic_hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	auto [automatic_runtime_rows_rc, automatic_runtime_rows] = bgd_runtime_rows(admin, automatic_hgs.blue_writer);
	ok(rc == EXIT_SUCCESS && automatic_available_rc == EXIT_SUCCESS && automatic_runtime_rows_rc == EXIT_SUCCESS &&
		automatic_runtime_rows.size() == 1 && automatic_runtime_rows[0].size() == 6 && automatic_runtime_rows[0][4] == "0",
		"automatic-ownership: discovery retains one administrator-owned runtime BGD row instead of replacing it");
	auto [automatic_bgd_after_rc, automatic_bgd_after] = mysql_query_ext_rows(admin,
		"SELECT writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup,active,writer_is_also_reader,check_interval_ms,check_timeout_ms,comment FROM mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=1310");
	ok(automatic_admin_snapshot_rc == EXIT_SUCCESS && automatic_runtime_snapshot_rc == EXIT_SUCCESS &&
		automatic_bgd_snapshot_rc == EXIT_SUCCESS && automatic_bgd_after_rc == EXIT_SUCCESS &&
		automatic_bgd_after == automatic_bgd_snapshot && snapshots_unchanged(admin, automatic_hgs,
			automatic_admin_snapshot, automatic_runtime_snapshot),
		"automatic-ownership: discovery never overwrites administrator-owned BGD configuration or green rows/statuses");

	if (reset_scenario(admin, sim, automatic_backends) != EXIT_SUCCESS) diag("failed to clean final BGD server-policy scenario");
	mysql_close(admin);
	return exit_status();
}
