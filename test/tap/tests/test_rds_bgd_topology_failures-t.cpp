/**
 * @file test_rds_bgd_topology_failures-t.cpp
 * @brief AWS RDS Blue/Green topology absence and metadata-failure coverage.
 *
 * Test coverage:
 * 1. Distinguishes present-empty and absent topology before completion.
 * 2. Applies successful cleanup for both conditions during reader switchover.
 * 3. Treats metadata error 1146 as topology absence in both lifecycle regions.
 * 4. Preserves the active phase for a generic metadata failure.
 *
 * The scenarios verify public placement, connection-pool effects, retained
 * configured rows, and the observable metadata-versus-table-check probe path.
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
const uint32_t kNegativeTimeoutSeconds = 1;

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

void set_read_only(RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster) {
	if (sim.read_only_update(cluster.blue_writer.host_endpoint(), false) != EXIT_SUCCESS ||
		sim.read_only_update(cluster.green_writer.host_endpoint(), false) != EXIT_SUCCESS ||
		sim.read_only_update(cluster.blue_readers[0].host_endpoint(), true) != EXIT_SUCCESS ||
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

int wait_for_precompletion_effects(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster, bool demoted)
{
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=" +
		string(demoted ? "0" : "1") + " AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=" +
		string(demoted ? "1" : "0") + " AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[1].hostname) +
		" AND port=3306 AND status='ONLINE')=1";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario,
		demoted ? "in-progress" : "rollback", demoted ? "blue writer demotion" : "blue writer restoration",
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

int set_default_hostgroup(MYSQL* admin, int hostgroup) {
	return execute_all(admin, {
		"UPDATE mysql_users SET default_hostgroup=" + to_string(hostgroup) + " WHERE username='testuser'",
		"LOAD MYSQL USERS TO RUNTIME",
	});
}

int establish_green_pools(const CommandLine& cl, MYSQL* admin, const BGD_Hostgroups& hgs) {
	MYSQL* writer_client = nullptr;
	MYSQL* reader_client = nullptr;
	int rc = set_default_hostgroup(admin, hgs.green_writer);
	if (rc == EXIT_SUCCESS) writer_client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	if (writer_client != nullptr && bgd_backend_ip_echo(writer_client).first != EXIT_SUCCESS) rc = EXIT_FAILURE;
	if (writer_client != nullptr) mysql_close(writer_client);
	if (rc == EXIT_SUCCESS && writer_client == nullptr) rc = EXIT_FAILURE;
	if (rc == EXIT_SUCCESS) rc = set_default_hostgroup(admin, hgs.green_reader);
	if (rc == EXIT_SUCCESS) reader_client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	if (reader_client != nullptr && bgd_backend_ip_echo(reader_client).first != EXIT_SUCCESS) rc = EXIT_FAILURE;
	if (reader_client != nullptr) mysql_close(reader_client);
	if (rc == EXIT_SUCCESS && reader_client == nullptr) rc = EXIT_FAILURE;
	if (set_default_hostgroup(admin, hgs.blue_writer) != EXIT_SUCCESS) rc = EXIT_FAILURE;
	return rc;
}

int wait_for_green_pool_baseline(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, const string& phase)
{
	const string query = "SELECT "
		"(SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hgs.green_writer) + ")>0 AND "
		"(SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hgs.green_reader) + ")>0";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, phase,
		"green writer and reader pool baselines", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

int wait_for_green_pool_drain(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, const string& phase)
{
	const string query = "SELECT "
		"(SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hgs.green_writer) + ")=0 AND "
		"(SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hgs.green_reader) + ")=0";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, phase,
		"green writer and reader pools drained", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

int enter_in_progress(MYSQL* admin, RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster,
	const BGD_Hostgroups& hgs, const string& scenario, uint64_t& sequence)
{
	const vector<Endpoint> backends = topology_backends(cluster);
	auto [available_seq_rc, available_seq] = sim.probe_log_last_sequence();
	int rc = available_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_reader_pair(cluster, "AVAILABLE")) : EXIT_FAILURE;
	if (rc != EXIT_SUCCESS || wait_for_status(admin, sim, available_seq, scenario, hgs, "available", "AVAILABLE") != EXIT_SUCCESS) return EXIT_FAILURE;
	auto [progress_seq_rc, progress_seq] = sim.probe_log_last_sequence();
	rc = progress_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_reader_pair(cluster, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	if (rc != EXIT_SUCCESS || wait_for_status(admin, sim, progress_seq, scenario, hgs, "in-progress", "WRITER_SWITCHOVER_IN_PROGRESS") != EXIT_SUCCESS ||
		wait_for_precompletion_effects(admin, sim, progress_seq, scenario, hgs, cluster, true) != EXIT_SUCCESS) return EXIT_FAILURE;
	sequence = progress_seq;
	return EXIT_SUCCESS;
}

int enter_reader_switchover(MYSQL* admin, RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster,
	const BGD_Hostgroups& hgs, const string& scenario, uint64_t& sequence)
{
	if (enter_in_progress(admin, sim, cluster, hgs, scenario, sequence) != EXIT_SUCCESS) return EXIT_FAILURE;
	const vector<Endpoint> backends = topology_backends(cluster);
	auto [post_seq_rc, post_seq] = sim.probe_log_last_sequence();
	int rc = post_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, topology_with_reader_pair(cluster, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	if (rc != EXIT_SUCCESS || wait_for_status(admin, sim, post_seq, scenario, hgs, "post-processing", "WRITER_SWITCHOVER_POST_PROCESSING") != EXIT_SUCCESS ||
		bgd_wait_for_condition(admin,
			"SELECT "
			"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
			" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306 AND status='ONLINE')=1 AND "
			"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
			" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=0 AND "
			"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
			" AND hostname=" + bgd_sql_quote(cluster.blue_readers[0].hostname) + " AND port=3306 AND status='ONLINE')=1",
			kTimeoutSeconds, sim, post_seq, scenario, "post-processing placement",
			"writer restored and mapped reader remains ONLINE",
			hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader }) != EXIT_SUCCESS) return EXIT_FAILURE;
	auto [completed_seq_rc, completed_seq] = sim.probe_log_last_sequence();
	rc = completed_seq_rc == EXIT_SUCCESS ? sim.topology_update(backends, target_only_completed(cluster)) : EXIT_FAILURE;
	if (rc != EXIT_SUCCESS || wait_for_status(admin, sim, completed_seq, scenario, hgs, "writer-completed", "READER_SWITCHOVER_IN_PROGRESS") != EXIT_SUCCESS) return EXIT_FAILURE;
	sequence = completed_seq;
	return EXIT_SUCCESS;
}

bool green_rows_remain(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster) {
	return server_has_status(admin, hgs.green_writer, cluster.green_writer, "ONLINE") &&
		server_has_status(admin, hgs.green_reader, cluster.green_readers[0], "ONLINE");
}

bool telemetry_has_kind(RDS_BGD_Simulator& sim, uint64_t sequence, Endpoint backend, RDS_BGD_Probe_Kind kind) {
	auto [rc, count] = bgd_probe_count_since(sim, sequence, backend, kind);
	return rc == EXIT_SUCCESS && count > 0;
}

/**
 * Delete topology rows before completion while the table remains present and
 * verify blue rollback without green pool cleanup.
 */
void test_present_empty_before_completion(const CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Enter in-progress with nonzero green pools, then publish an empty topology.
	RDS_BGD_Cluster empty_pre = bgd_cluster_init();
	BGD_Hostgroups empty_pre_hgs { 1100, 1101, 1102, 1103 };
	vector<Endpoint> empty_pre_backends = topology_backends(empty_pre);
	if (reset_scenario(admin, sim, empty_pre_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset present-empty rollback scenario");
	set_read_only(sim, empty_pre);
	if (bgd_admin_setup(admin, empty_pre, empty_pre_hgs, BGD_Admin_Mode::explicit_configuration,
		{ empty_pre.blue_writer, empty_pre.blue_readers[0], empty_pre.blue_readers[1] },
		{ empty_pre.green_writer, empty_pre.green_readers[0] }) != EXIT_SUCCESS) BAIL_OUT("failed to configure present-empty rollback scenario");
	uint64_t sequence = 0;
	int rc = enter_in_progress(admin, sim, empty_pre, empty_pre_hgs, "precompletion-present-empty", sequence);
	int pools_rc = rc == EXIT_SUCCESS ? establish_green_pools(cl, admin, empty_pre_hgs) : EXIT_FAILURE;
	auto [empty_pre_writer_pool_rc, empty_pre_writer_pool] = bgd_connection_pool_count(admin, empty_pre_hgs.green_writer);
	auto [empty_pre_reader_pool_rc, empty_pre_reader_pool] = bgd_connection_pool_count(admin, empty_pre_hgs.green_reader);
	ok(rc == EXIT_SUCCESS && pools_rc == EXIT_SUCCESS && empty_pre_writer_pool_rc == EXIT_SUCCESS && empty_pre_writer_pool >= 1 &&
		empty_pre_reader_pool_rc == EXIT_SUCCESS && empty_pre_reader_pool >= 1,
		"pre-completion present-empty scenario starts after demotion with green pools to distinguish rollback");
	auto [empty_pre_seq_rc, empty_pre_seq] = sim.probe_log_last_sequence();
	rc = empty_pre_seq_rc == EXIT_SUCCESS ? sim.topology_delete(empty_pre_backends) : EXIT_FAILURE;
	int empty_pre_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, empty_pre_seq, "precompletion-present-empty", empty_pre_hgs, "present-empty", "NONE") : EXIT_FAILURE;
	auto [empty_pre_after_writer_rc, empty_pre_after_writer] = bgd_connection_pool_count(admin, empty_pre_hgs.green_writer);
	auto [empty_pre_after_reader_rc, empty_pre_after_reader] = bgd_connection_pool_count(admin, empty_pre_hgs.green_reader);
	ok(rc == EXIT_SUCCESS && empty_pre_none_rc == EXIT_SUCCESS &&
		wait_for_precompletion_effects(admin, sim, empty_pre_seq, "precompletion-present-empty", empty_pre_hgs, empty_pre, false) == EXIT_SUCCESS &&
		empty_pre_after_writer_rc == EXIT_SUCCESS && empty_pre_after_writer >= empty_pre_writer_pool &&
		empty_pre_after_reader_rc == EXIT_SUCCESS && empty_pre_after_reader >= empty_pre_reader_pool && green_rows_remain(admin, empty_pre_hgs, empty_pre),
		"pre-completion successful empty metadata rolls back blue placement and leaves green rows and pools intact");
	ok(telemetry_has_kind(sim, empty_pre_seq, empty_pre.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata),
		"present-empty topology records a successful metadata probe on the direct green backend");
}

/**
 * Drop the topology table before completion and verify the same rollback policy
 * through the distinguishable table-check path.
 */
void test_absent_before_completion(const CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Enter in-progress with nonzero green pools, then make topology absent.
	RDS_BGD_Cluster absent_pre = bgd_cluster_2_init();
	BGD_Hostgroups absent_pre_hgs { 1110, 1111, 1112, 1113 };
	vector<Endpoint> absent_pre_backends = topology_backends(absent_pre);
	if (reset_scenario(admin, sim, absent_pre_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset absent rollback scenario");
	set_read_only(sim, absent_pre);
	if (bgd_admin_setup(admin, absent_pre, absent_pre_hgs, BGD_Admin_Mode::explicit_configuration,
		{ absent_pre.blue_writer, absent_pre.blue_readers[0], absent_pre.blue_readers[1] },
		{ absent_pre.green_writer, absent_pre.green_readers[0] }) != EXIT_SUCCESS) BAIL_OUT("failed to configure absent rollback scenario");
	uint64_t sequence = 0;
	int rc = enter_in_progress(admin, sim, absent_pre, absent_pre_hgs, "precompletion-absent", sequence);
	int pools_rc = rc == EXIT_SUCCESS ? establish_green_pools(cl, admin, absent_pre_hgs) : EXIT_FAILURE;
	auto [absent_pre_writer_pool_rc, absent_pre_writer_pool] = bgd_connection_pool_count(admin, absent_pre_hgs.green_writer);
	auto [absent_pre_reader_pool_rc, absent_pre_reader_pool] = bgd_connection_pool_count(admin, absent_pre_hgs.green_reader);
	auto [absent_pre_seq_rc, absent_pre_seq] = sim.probe_log_last_sequence();
	rc = rc == EXIT_SUCCESS && absent_pre_seq_rc == EXIT_SUCCESS ? sim.topology_drop(absent_pre_backends) : EXIT_FAILURE;
	int absent_pre_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, absent_pre_seq, "precompletion-absent", absent_pre_hgs, "absent", "NONE") : EXIT_FAILURE;
	auto [absent_pre_after_writer_rc, absent_pre_after_writer] = bgd_connection_pool_count(admin, absent_pre_hgs.green_writer);
	auto [absent_pre_after_reader_rc, absent_pre_after_reader] = bgd_connection_pool_count(admin, absent_pre_hgs.green_reader);
	ok(pools_rc == EXIT_SUCCESS && absent_pre_writer_pool_rc == EXIT_SUCCESS && absent_pre_writer_pool >= 1 &&
		absent_pre_reader_pool_rc == EXIT_SUCCESS && absent_pre_reader_pool >= 1 && rc == EXIT_SUCCESS && absent_pre_none_rc == EXIT_SUCCESS &&
		wait_for_precompletion_effects(admin, sim, absent_pre_seq, "precompletion-absent", absent_pre_hgs, absent_pre, false) == EXIT_SUCCESS &&
		absent_pre_after_writer_rc == EXIT_SUCCESS && absent_pre_after_writer >= absent_pre_writer_pool &&
		absent_pre_after_reader_rc == EXIT_SUCCESS && absent_pre_after_reader >= absent_pre_reader_pool && green_rows_remain(admin, absent_pre_hgs, absent_pre),
		"pre-completion absent topology restores blue placement without draining green pools");
	ok(telemetry_has_kind(sim, absent_pre_seq, absent_pre.blue_writer.endpoint(), RDS_BGD_Probe_Kind::table_check),
		"dropped topology records an absent-table table-check instead of successful empty metadata");
}

/**
 * Delete topology rows during reader switchover and verify successful reader
 * restoration plus green pool draining.
 */
void test_present_empty_during_reader_switchover(
	const CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim)
{
	// Enter reader switchover with nonzero green pools, then empty the topology.
	RDS_BGD_Cluster empty_reader = bgd_cluster_3_init();
	BGD_Hostgroups empty_reader_hgs { 1120, 1121, 1122, 1123 };
	vector<Endpoint> empty_reader_backends = topology_backends(empty_reader);
	if (reset_scenario(admin, sim, empty_reader_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset reader present-empty scenario");
	set_read_only(sim, empty_reader);
	if (bgd_admin_setup(admin, empty_reader, empty_reader_hgs, BGD_Admin_Mode::explicit_configuration,
		{ empty_reader.blue_writer, empty_reader.blue_readers[0], empty_reader.blue_readers[1] },
		{ empty_reader.green_writer, empty_reader.green_readers[0] }) != EXIT_SUCCESS) BAIL_OUT("failed to configure reader present-empty scenario");
	uint64_t sequence = 0;
	int rc = enter_reader_switchover(admin, sim, empty_reader, empty_reader_hgs, "reader-present-empty", sequence);
	int pools_rc = rc == EXIT_SUCCESS ? establish_green_pools(cl, admin, empty_reader_hgs) : EXIT_FAILURE;
	int empty_reader_baseline_rc = pools_rc == EXIT_SUCCESS ? wait_for_green_pool_baseline(admin, sim, sequence,
		"reader-present-empty", empty_reader_hgs, "green pool baseline") : EXIT_FAILURE;
	auto [empty_reader_writer_pool_rc, empty_reader_writer_pool] = bgd_connection_pool_count(admin, empty_reader_hgs.green_writer);
	auto [empty_reader_reader_pool_rc, empty_reader_reader_pool] = bgd_connection_pool_count(admin, empty_reader_hgs.green_reader);
	auto [empty_reader_seq_rc, empty_reader_seq] = sim.probe_log_last_sequence();
	rc = rc == EXIT_SUCCESS && empty_reader_seq_rc == EXIT_SUCCESS ? sim.topology_delete(empty_reader_backends) : EXIT_FAILURE;
	int empty_reader_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, empty_reader_seq, "reader-present-empty", empty_reader_hgs, "present-empty", "NONE") : EXIT_FAILURE;
	int empty_reader_drain_rc = empty_reader_none_rc == EXIT_SUCCESS ? wait_for_green_pool_drain(admin, sim, empty_reader_seq,
		"reader-present-empty", empty_reader_hgs, "successful cleanup") : EXIT_FAILURE;
	auto [empty_reader_after_writer_rc, empty_reader_after_writer] = bgd_connection_pool_count(admin, empty_reader_hgs.green_writer);
	auto [empty_reader_after_reader_rc, empty_reader_after_reader] = bgd_connection_pool_count(admin, empty_reader_hgs.green_reader);
	ok(pools_rc == EXIT_SUCCESS && empty_reader_baseline_rc == EXIT_SUCCESS && empty_reader_writer_pool_rc == EXIT_SUCCESS && empty_reader_writer_pool >= 1 &&
		empty_reader_reader_pool_rc == EXIT_SUCCESS && empty_reader_reader_pool >= 1 && rc == EXIT_SUCCESS && empty_reader_none_rc == EXIT_SUCCESS &&
		empty_reader_drain_rc == EXIT_SUCCESS && empty_reader_after_writer_rc == EXIT_SUCCESS && empty_reader_after_writer == 0 &&
		empty_reader_after_reader_rc == EXIT_SUCCESS && empty_reader_after_reader == 0 &&
		server_has_status(admin, empty_reader_hgs.blue_reader, empty_reader.blue_readers[1], "ONLINE") && green_rows_remain(admin, empty_reader_hgs, empty_reader),
		"reader-switchover successful empty metadata restores readers, drains both green pools, and retains green rows");
	ok(telemetry_has_kind(sim, empty_reader_seq, empty_reader.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata),
		"reader-switchover empty topology is observed through successful direct metadata");
}

/**
 * Drop the topology table during reader switchover and verify successful
 * cleanup through the table-check path.
 */
void test_absent_during_reader_switchover(
	const CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim)
{
	// Enter reader switchover with nonzero green pools, then drop topology.
	RDS_BGD_Cluster absent_reader = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups absent_reader_hgs { 1130, 1131, 1132, 1133 };
	vector<Endpoint> absent_reader_backends = topology_backends(absent_reader);
	if (reset_scenario(admin, sim, absent_reader_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset reader absent scenario");
	set_read_only(sim, absent_reader);
	if (bgd_admin_setup(admin, absent_reader, absent_reader_hgs, BGD_Admin_Mode::explicit_configuration,
		{ absent_reader.blue_writer, absent_reader.blue_readers[0], absent_reader.blue_readers[1] },
		{ absent_reader.green_writer, absent_reader.green_readers[0] }) != EXIT_SUCCESS) BAIL_OUT("failed to configure reader absent scenario");
	uint64_t sequence = 0;
	int rc = enter_reader_switchover(admin, sim, absent_reader, absent_reader_hgs, "reader-absent", sequence);
	int pools_rc = rc == EXIT_SUCCESS ? establish_green_pools(cl, admin, absent_reader_hgs) : EXIT_FAILURE;
	int absent_reader_baseline_rc = pools_rc == EXIT_SUCCESS ? wait_for_green_pool_baseline(admin, sim, sequence,
		"reader-absent", absent_reader_hgs, "green pool baseline") : EXIT_FAILURE;
	auto [absent_reader_writer_pool_rc, absent_reader_writer_pool] = bgd_connection_pool_count(admin, absent_reader_hgs.green_writer);
	auto [absent_reader_reader_pool_rc, absent_reader_reader_pool] = bgd_connection_pool_count(admin, absent_reader_hgs.green_reader);
	auto [absent_reader_seq_rc, absent_reader_seq] = sim.probe_log_last_sequence();
	rc = rc == EXIT_SUCCESS && absent_reader_seq_rc == EXIT_SUCCESS ? sim.topology_drop(absent_reader_backends) : EXIT_FAILURE;
	int absent_reader_none_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, absent_reader_seq, "reader-absent", absent_reader_hgs, "absent", "NONE") : EXIT_FAILURE;
	int absent_reader_drain_rc = absent_reader_none_rc == EXIT_SUCCESS ? wait_for_green_pool_drain(admin, sim, absent_reader_seq,
		"reader-absent", absent_reader_hgs, "successful cleanup") : EXIT_FAILURE;
	auto [absent_reader_after_writer_rc, absent_reader_after_writer] = bgd_connection_pool_count(admin, absent_reader_hgs.green_writer);
	auto [absent_reader_after_reader_rc, absent_reader_after_reader] = bgd_connection_pool_count(admin, absent_reader_hgs.green_reader);
	ok(pools_rc == EXIT_SUCCESS && absent_reader_baseline_rc == EXIT_SUCCESS && absent_reader_writer_pool_rc == EXIT_SUCCESS && absent_reader_writer_pool >= 1 &&
		absent_reader_reader_pool_rc == EXIT_SUCCESS && absent_reader_reader_pool >= 1 && rc == EXIT_SUCCESS && absent_reader_none_rc == EXIT_SUCCESS &&
		absent_reader_drain_rc == EXIT_SUCCESS && absent_reader_after_writer_rc == EXIT_SUCCESS && absent_reader_after_writer == 0 &&
		absent_reader_after_reader_rc == EXIT_SUCCESS && absent_reader_after_reader == 0 &&
		server_has_status(admin, absent_reader_hgs.blue_reader, absent_reader.blue_readers[1], "ONLINE") && green_rows_remain(admin, absent_reader_hgs, absent_reader),
		"reader-switchover absent topology drains both green pools and performs successful cleanup rather than blue rollback");
	ok(telemetry_has_kind(sim, absent_reader_seq, absent_reader.blue_writer.endpoint(), RDS_BGD_Probe_Kind::table_check),
		"reader-switchover dropped topology remains distinguishable as a table-check observation");
}

/**
 * Return metadata error 1146 before completion and verify immediate rollback
 * followed by a return to table checking.
 */
void test_metadata_1146_before_completion(MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Enter in-progress, inject 1146 on the direct green probe, then drop blue topology.
	RDS_BGD_Cluster error_1146_pre = bgd_cluster_init();
	BGD_Hostgroups error_1146_pre_hgs { 1140, 1141, 1142, 1143 };
	vector<Endpoint> error_1146_pre_backends = topology_backends(error_1146_pre);
	if (reset_scenario(admin, sim, error_1146_pre_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset 1146 rollback scenario");
	set_read_only(sim, error_1146_pre);
	if (bgd_admin_setup(admin, error_1146_pre, error_1146_pre_hgs, BGD_Admin_Mode::explicit_configuration,
		{ error_1146_pre.blue_writer, error_1146_pre.blue_readers[0], error_1146_pre.blue_readers[1] },
		{ error_1146_pre.green_writer, error_1146_pre.green_readers[0] }) != EXIT_SUCCESS) BAIL_OUT("failed to configure 1146 rollback scenario");
	uint64_t sequence = 0;
	int rc = enter_in_progress(admin, sim, error_1146_pre, error_1146_pre_hgs, "metadata-1146-precompletion", sequence);
	auto [error_1146_pre_seq_rc, error_1146_pre_seq] = sim.probe_log_last_sequence();
	rc = rc == EXIT_SUCCESS && error_1146_pre_seq_rc == EXIT_SUCCESS ?
		sim.topology_error({ error_1146_pre.green_writer.endpoint() }, 1146, "Table 'mysql.rds_topology' doesn't exist") : EXIT_FAILURE;
	auto [error_1146_metadata_rc, error_1146_metadata] = rc == EXIT_SUCCESS ? bgd_wait_for_probe(sim, error_1146_pre_seq,
		error_1146_pre.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin,
		"metadata-1146-precompletion", "1146 metadata", error_1146_pre_hgs.blue_writer,
		{ error_1146_pre_hgs.blue_writer, error_1146_pre_hgs.blue_reader, error_1146_pre_hgs.green_writer, error_1146_pre_hgs.green_reader }) : rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	int error_1146_none_rc = error_1146_metadata_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, error_1146_metadata.sequence_id,
		"metadata-1146-precompletion", error_1146_pre_hgs, "metadata absence", "NONE") : EXIT_FAILURE;
	int error_1146_rollback_rc = error_1146_none_rc == EXIT_SUCCESS ? wait_for_precompletion_effects(admin, sim,
		error_1146_metadata.sequence_id, "metadata-1146-precompletion", error_1146_pre_hgs, error_1146_pre, false) : EXIT_FAILURE;
	int drop_after_1146_rc = error_1146_rollback_rc == EXIT_SUCCESS ? sim.topology_drop({ error_1146_pre.blue_writer.endpoint() }) : EXIT_FAILURE;
	const uint64_t table_baseline = error_1146_metadata_rc == EXIT_SUCCESS ? error_1146_metadata.sequence_id : error_1146_pre_seq;
	auto [error_1146_table_rc, error_1146_table] = drop_after_1146_rc == EXIT_SUCCESS ? bgd_wait_for_probe(sim, table_baseline,
		error_1146_pre.blue_writer.endpoint(), RDS_BGD_Probe_Kind::table_check, kProbeTimeoutMs, 0, admin,
		"metadata-1146-precompletion", "post-cleanup table check", error_1146_pre_hgs.blue_writer,
		{ error_1146_pre_hgs.blue_writer, error_1146_pre_hgs.blue_reader, error_1146_pre_hgs.green_writer, error_1146_pre_hgs.green_reader }) : rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	ok(rc == EXIT_SUCCESS && error_1146_metadata_rc == EXIT_SUCCESS && drop_after_1146_rc == EXIT_SUCCESS &&
		error_1146_none_rc == EXIT_SUCCESS && error_1146_rollback_rc == EXIT_SUCCESS && error_1146_table_rc == EXIT_SUCCESS &&
		error_1146_metadata.sequence_id < error_1146_table.sequence_id,
		"metadata error 1146 immediately applies pre-completion rollback, then returns to table checking");
}

/**
 * Return metadata error 1146 during reader switchover and verify successful
 * cleanup before table checking resumes.
 */
void test_metadata_1146_during_reader_switchover(
	const CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim)
{
	// Enter reader switchover with green pools, then inject 1146 on direct metadata.
	RDS_BGD_Cluster error_1146_reader = bgd_cluster_2_init();
	BGD_Hostgroups error_1146_reader_hgs { 1150, 1151, 1152, 1153 };
	vector<Endpoint> error_1146_reader_backends = topology_backends(error_1146_reader);
	if (reset_scenario(admin, sim, error_1146_reader_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset 1146 reader scenario");
	set_read_only(sim, error_1146_reader);
	if (bgd_admin_setup(admin, error_1146_reader, error_1146_reader_hgs, BGD_Admin_Mode::explicit_configuration,
		{ error_1146_reader.blue_writer, error_1146_reader.blue_readers[0], error_1146_reader.blue_readers[1] },
		{ error_1146_reader.green_writer, error_1146_reader.green_readers[0] }) != EXIT_SUCCESS) BAIL_OUT("failed to configure 1146 reader scenario");
	uint64_t sequence = 0;
	int rc = enter_reader_switchover(admin, sim, error_1146_reader, error_1146_reader_hgs, "metadata-1146-reader", sequence);
	int pools_rc = rc == EXIT_SUCCESS ? establish_green_pools(cl, admin, error_1146_reader_hgs) : EXIT_FAILURE;
	int error_1146_reader_baseline_rc = pools_rc == EXIT_SUCCESS ? wait_for_green_pool_baseline(admin, sim, sequence,
		"metadata-1146-reader", error_1146_reader_hgs, "green pool baseline") : EXIT_FAILURE;
	auto [error_1146_reader_writer_pool_rc, error_1146_reader_writer_pool] = bgd_connection_pool_count(admin, error_1146_reader_hgs.green_writer);
	auto [error_1146_reader_reader_pool_rc, error_1146_reader_reader_pool] = bgd_connection_pool_count(admin, error_1146_reader_hgs.green_reader);
	auto [error_1146_reader_seq_rc, error_1146_reader_seq] = sim.probe_log_last_sequence();
	rc = rc == EXIT_SUCCESS && error_1146_reader_seq_rc == EXIT_SUCCESS ?
		sim.topology_error({ error_1146_reader.green_writer.endpoint() }, 1146, "Table 'mysql.rds_topology' doesn't exist") : EXIT_FAILURE;
	auto [error_1146_reader_metadata_rc, error_1146_reader_metadata] = rc == EXIT_SUCCESS ? bgd_wait_for_probe(sim, error_1146_reader_seq,
		error_1146_reader.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin,
		"metadata-1146-reader", "1146 metadata", error_1146_reader_hgs.blue_writer,
		{ error_1146_reader_hgs.blue_writer, error_1146_reader_hgs.blue_reader, error_1146_reader_hgs.green_writer, error_1146_reader_hgs.green_reader }) : rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	int error_1146_reader_none_rc = error_1146_reader_metadata_rc == EXIT_SUCCESS ? wait_for_status(admin, sim,
		error_1146_reader_metadata.sequence_id, "metadata-1146-reader", error_1146_reader_hgs, "metadata absence", "NONE") : EXIT_FAILURE;
	int error_1146_reader_drain_rc = error_1146_reader_none_rc == EXIT_SUCCESS ? wait_for_green_pool_drain(admin, sim,
		error_1146_reader_metadata.sequence_id, "metadata-1146-reader", error_1146_reader_hgs, "successful cleanup") : EXIT_FAILURE;
	auto [error_1146_reader_after_writer_rc, error_1146_reader_after_writer] = bgd_connection_pool_count(admin, error_1146_reader_hgs.green_writer);
	auto [error_1146_reader_after_reader_rc, error_1146_reader_after_reader] = bgd_connection_pool_count(admin, error_1146_reader_hgs.green_reader);
	int drop_after_1146_rc = error_1146_reader_drain_rc == EXIT_SUCCESS ?
		sim.topology_drop({ error_1146_reader.blue_writer.endpoint() }) : EXIT_FAILURE;
	const uint64_t reader_table_baseline = error_1146_reader_metadata_rc == EXIT_SUCCESS ? error_1146_reader_metadata.sequence_id : error_1146_reader_seq;
	auto [error_1146_reader_table_rc, error_1146_reader_table] = drop_after_1146_rc == EXIT_SUCCESS ? bgd_wait_for_probe(sim, reader_table_baseline,
		error_1146_reader.blue_writer.endpoint(), RDS_BGD_Probe_Kind::table_check, kProbeTimeoutMs, 0, admin,
		"metadata-1146-reader", "post-cleanup table check", error_1146_reader_hgs.blue_writer,
		{ error_1146_reader_hgs.blue_writer, error_1146_reader_hgs.blue_reader, error_1146_reader_hgs.green_writer, error_1146_reader_hgs.green_reader }) : rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	ok(pools_rc == EXIT_SUCCESS && error_1146_reader_baseline_rc == EXIT_SUCCESS &&
		error_1146_reader_writer_pool_rc == EXIT_SUCCESS && error_1146_reader_writer_pool >= 1 &&
		error_1146_reader_reader_pool_rc == EXIT_SUCCESS && error_1146_reader_reader_pool >= 1 && rc == EXIT_SUCCESS &&
		error_1146_reader_metadata_rc == EXIT_SUCCESS && error_1146_reader_none_rc == EXIT_SUCCESS && error_1146_reader_drain_rc == EXIT_SUCCESS &&
		error_1146_reader_after_writer_rc == EXIT_SUCCESS && error_1146_reader_after_writer == 0 &&
		error_1146_reader_after_reader_rc == EXIT_SUCCESS && error_1146_reader_after_reader == 0 &&
		server_has_status(admin, error_1146_reader_hgs.blue_reader, error_1146_reader.blue_readers[1], "ONLINE") &&
		green_rows_remain(admin, error_1146_reader_hgs, error_1146_reader) && drop_after_1146_rc == EXIT_SUCCESS &&
			error_1146_reader_table_rc == EXIT_SUCCESS && error_1146_reader_metadata.sequence_id < error_1146_reader_table.sequence_id,
		"metadata error 1146 immediately performs reader cleanup, then returns to table checking");
}

/**
 * Return a generic metadata error during in-progress and verify that it neither
 * clears the phase nor follows the absent-topology path.
 */
void test_generic_metadata_failure(MYSQL* admin, RDS_BGD_Simulator& sim) {
	// Enter in-progress, inject a generic direct-metadata failure, and observe the active state.
	RDS_BGD_Cluster generic_error = bgd_cluster_3_init();
	BGD_Hostgroups generic_error_hgs { 1160, 1161, 1162, 1163 };
	vector<Endpoint> generic_error_backends = topology_backends(generic_error);
	if (reset_scenario(admin, sim, generic_error_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset generic metadata error scenario");
	set_read_only(sim, generic_error);
	if (bgd_admin_setup(admin, generic_error, generic_error_hgs, BGD_Admin_Mode::explicit_configuration,
		{ generic_error.blue_writer, generic_error.blue_readers[0], generic_error.blue_readers[1] },
		{ generic_error.green_writer, generic_error.green_readers[0] }) != EXIT_SUCCESS) BAIL_OUT("failed to configure generic metadata error scenario");
	uint64_t sequence = 0;
	int rc = enter_in_progress(admin, sim, generic_error, generic_error_hgs, "generic-metadata-error", sequence);
	auto [generic_seq_rc, generic_seq] = sim.probe_log_last_sequence();
	rc = rc == EXIT_SUCCESS && generic_seq_rc == EXIT_SUCCESS ?
		sim.topology_error({ generic_error.green_writer.endpoint() }, 1105, "simulated generic metadata failure") : EXIT_FAILURE;
	auto [generic_metadata_rc, generic_metadata] = rc == EXIT_SUCCESS ? bgd_wait_for_probe(sim, generic_seq,
		generic_error.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin,
		"generic-metadata-error", "generic metadata", generic_error_hgs.blue_writer,
		{ generic_error_hgs.blue_writer, generic_error_hgs.blue_reader, generic_error_hgs.green_writer, generic_error_hgs.green_reader }) : rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	int no_none_rc = generic_metadata_rc == EXIT_SUCCESS ? wait_for_cond(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" + to_string(generic_error_hgs.blue_writer) +
		" AND status='NONE'", kNegativeTimeoutSeconds) : EXIT_FAILURE;
	int generic_active_rc = generic_metadata_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, generic_metadata.sequence_id,
		"generic-metadata-error", generic_error_hgs, "generic metadata", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && generic_metadata_rc == EXIT_SUCCESS && no_none_rc != EXIT_SUCCESS && generic_active_rc == EXIT_SUCCESS &&
		server_absent(admin, generic_error_hgs.blue_writer, generic_error.blue_writer) &&
		server_has_status(admin, generic_error_hgs.blue_reader, generic_error.blue_writer, "ONLINE"),
		"generic metadata failure is not absence and does not clear or advance the active in-progress effects");
	ok(telemetry_has_kind(sim, generic_seq, generic_error.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata) &&
		!telemetry_has_kind(sim, generic_seq, generic_error.green_writer.endpoint(), RDS_BGD_Probe_Kind::table_check),
		"generic metadata failure remains a metadata telemetry path rather than an absent-table path");
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
	if (bgd_register_test_cleanup(admin, sim) != EXIT_SUCCESS) BAIL_OUT("failed to register BGD TAP cleanup");

	test_present_empty_before_completion(cl, admin, sim);
	test_absent_before_completion(cl, admin, sim);
	test_present_empty_during_reader_switchover(cl, admin, sim);
	test_absent_during_reader_switchover(cl, admin, sim);
	test_metadata_1146_before_completion(admin, sim);
	test_metadata_1146_during_reader_switchover(cl, admin, sim);
	test_generic_metadata_failure(admin, sim);

	int cleanup_rc = bgd_finish_test_cleanup(admin, sim);
	if (cleanup_rc != EXIT_SUCCESS) BAIL_OUT("failed to clean final topology-failure TAP state");
	mysql_close(admin);
	return exit_status();
}
