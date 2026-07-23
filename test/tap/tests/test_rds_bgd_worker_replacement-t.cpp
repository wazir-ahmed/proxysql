/**
 * @file test_rds_bgd_worker_replacement-t.cpp
 * @brief AWS RDS Blue/Green worker replacement coverage through runtime behavior.
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
const uint32_t kNoReplacementTimeoutMs = 800;

vector<Endpoint> scenario_backends(RDS_BGD_Cluster& cluster) {
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

int wait_for_writer_placement(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster,
	const string& phase, bool demoted)
{
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=" +
		string(demoted ? "0" : "1") + " AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=" +
		string(demoted ? "1" : "0");
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, phase,
		demoted ? "blue writer is demoted into the reader hostgroup" : "blue writer is restored to its writer hostgroup",
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

bool runtime_definition_matches(MYSQL* admin, const BGD_Hostgroups& hgs, int active,
	int writer_is_also_reader, int interval_ms, int timeout_ms)
{
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup,active,writer_is_also_reader,"
		"check_interval_ms,check_timeout_ms FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 7 &&
		rows[0][0] == to_string(hgs.blue_reader) && rows[0][1] == to_string(hgs.green_writer) &&
		rows[0][2] == to_string(hgs.green_reader) && rows[0][3] == to_string(active) &&
		rows[0][4] == to_string(writer_is_also_reader) && rows[0][5] == to_string(interval_ms) &&
		rows[0][6] == to_string(timeout_ms);
}

bool runtime_server_absent(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

bool persistent_server_status(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, const string& status) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT status FROM mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == status;
}

bool runtime_server_ssl(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, int use_ssl) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT use_ssl FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == to_string(use_ssl);
}

bool runtime_definition_absent(MYSQL* admin, int writer_hostgroup) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" + to_string(writer_hostgroup));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

bool create_blue_pool(const CommandLine& cl, MYSQL* admin, RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs) {
	MYSQL* client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	auto [echo_rc, echo] = client ? bgd_backend_ip_echo(client) : rc_t<string> { EXIT_FAILURE, {} };
	if (client) mysql_close(client);
	auto [pool_rc, pool] = bgd_connection_pool_count(admin, hgs.blue_writer, cluster.blue_writer.hostname);
	return echo_rc == EXIT_SUCCESS && echo.find(cluster.blue_writer.ip) != string::npos &&
		pool_rc == EXIT_SUCCESS && pool >= 1;
}

struct Replacement_Probe_Chain {
	int table_rc;
	RDS_BGD_Probe_Log table;
	int green_rc;
	RDS_BGD_Probe_Log green;
};

Replacement_Probe_Chain wait_for_replacement_probe_chain(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs, int green_ssl)
{
	auto [table_rc, table] = bgd_wait_for_probe(sim, sequence, cluster.blue_writer.endpoint(),
		RDS_BGD_Probe_Kind::table_check, kProbeTimeoutMs, 0, admin, scenario, "replacement table check",
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	const uint64_t blue_baseline = table_rc == EXIT_SUCCESS ? table.sequence_id : sequence;
	auto [blue_metadata_rc, blue_metadata] = bgd_wait_for_probe(sim, blue_baseline,
		cluster.blue_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin,
		scenario, "fresh blue metadata", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	const uint64_t green_baseline = blue_metadata_rc == EXIT_SUCCESS ? blue_metadata.sequence_id : blue_baseline;
	auto [green_rc, green] = bgd_wait_for_probe(sim, green_baseline,
		cluster.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, green_ssl, admin,
		scenario, "fresh green metadata", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	return { table_rc, table, green_rc, green };
}

int wait_for_replacement_table(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs)
{
	return bgd_wait_for_probe(sim, sequence, cluster.blue_writer.endpoint(), RDS_BGD_Probe_Kind::table_check,
		kProbeTimeoutMs, 0, admin, scenario, "replacement table check", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader }).first;
}

}  // namespace

int main() {
	plan(23);

	CommandLine cl {};
	if (cl.getEnv()) BAIL_OUT("failed to load TAP environment");
	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) BAIL_OUT("failed to connect to ProxySQL Admin");
	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}

	// Changing active BGD input during a pre-completion phase stops the old worker.  Its
	// observable cleanup must restore blue placement before a new definition is enabled.
	RDS_BGD_Cluster replacement = bgd_cluster_init();
	RDS_BGD_Cluster replacement_fresh = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups original_hgs { 1340, 1341, 1342, 1343 };
	BGD_Hostgroups replacement_hgs { 1340, 1341, 1344, 1345 };
	vector<Endpoint> replacement_backends = scenario_backends(replacement);
	for (RDS_BGD_Host& host : replacement_fresh.green_readers) replacement_backends.push_back(host.endpoint());
	replacement_backends.push_back(replacement_fresh.green_writer.endpoint());
	if (reset_scenario(admin, sim, replacement_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset replacement-input scenario");
	set_writers_writable(sim, replacement);
	set_writers_writable(sim, replacement_fresh);
	auto [initial_seq_rc, initial_seq] = sim.probe_log_last_sequence();
	int rc = initial_seq_rc == EXIT_SUCCESS ? sim.topology_update(replacement_backends,
		topology_with_readers(replacement, "AVAILABLE")) : EXIT_FAILURE;
	int setup_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, replacement, original_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ replacement.blue_writer, replacement.blue_readers[0], replacement.blue_readers[1] },
		{ replacement.green_writer, replacement.green_readers[0], replacement.green_readers[1] }, 0, 0) : EXIT_FAILURE;
	int initial_available_rc = setup_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, initial_seq,
		"replacement-input", original_hgs, "initial available", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && setup_rc == EXIT_SUCCESS && initial_available_rc == EXIT_SUCCESS,
		"pre-completion worker publishes the recorded AVAILABLE state");
	ok(initial_available_rc == EXIT_SUCCESS && create_blue_pool(cl, admin, replacement, original_hgs),
		"AVAILABLE worker retains a blue-writer client backend and pool before replacement cleanup");

	auto [initial_progress_seq_rc, initial_progress_seq] = sim.probe_log_last_sequence();
	rc = initial_progress_seq_rc == EXIT_SUCCESS ? sim.topology_update(replacement_backends,
		topology_with_readers(replacement, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	int initial_status_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, initial_progress_seq,
		"replacement-input", original_hgs, "initial in-progress", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	int initial_effects_rc = initial_status_rc == EXIT_SUCCESS ? wait_for_writer_placement(admin, sim, initial_progress_seq,
		"replacement-input", original_hgs, replacement, "initial in-progress effects", true) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initial_status_rc == EXIT_SUCCESS,
		"pre-completion worker publishes the recorded in-progress state");
	ok(initial_effects_rc == EXIT_SUCCESS,
		"in-progress policy demotes the blue writer into the reader hostgroup");

	// Alter every relevant input class while disabling the row: replace green HGs, add a
	// new eligible member, remove one old member, take another offline, change TLS and the
	// BGD options.  The disabled definition keeps the old cleanup observable.
	rc = bgd_admin_add_servers(admin, replacement_fresh, replacement_hgs,
		{ replacement_fresh.green_writer, replacement_fresh.green_readers[0] }, true, 1);
	if (rc == EXIT_SUCCESS) rc = execute_all(admin, {
		"UPDATE mysql_servers SET use_ssl=1,status='OFFLINE_SOFT' WHERE hostgroup_id=1343 AND hostname=" +
			bgd_sql_quote(replacement.green_readers[0].hostname) + " AND port=3306",
		"DELETE FROM mysql_servers WHERE hostgroup_id=1342 AND hostname=" +
			bgd_sql_quote(replacement.green_writer.hostname) + " AND port=3306",
		"DELETE FROM mysql_servers WHERE hostgroup_id=1343 AND hostname=" +
			bgd_sql_quote(replacement.green_readers[1].hostname) + " AND port=3306",
		"UPDATE mysql_aws_rds_bgd_hostgroups SET green_writer_hostgroup=1344,green_reader_hostgroup=1345,"
			"active=0,writer_is_also_reader=1,check_interval_ms=150,check_timeout_ms=900 WHERE writer_hostgroup=1340",
		"LOAD MYSQL SERVERS TO RUNTIME",
	});
	auto [disabled_seq_rc, disabled_seq] = sim.probe_log_last_sequence();
	int rollback_rc = rc == EXIT_SUCCESS && disabled_seq_rc == EXIT_SUCCESS ? wait_for_writer_placement(admin, sim, disabled_seq,
		"replacement-input", replacement_hgs, replacement, "disabled old worker", false) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && rollback_rc == EXIT_SUCCESS,
		"disabling changed pre-completion input performs the departing worker's one-shot rollback");
	auto [post_rollback_pool_rc, post_rollback_pool] = bgd_connection_pool_count(admin, original_hgs.blue_writer,
		replacement.blue_writer.hostname);
	ok(post_rollback_pool_rc == EXIT_SUCCESS && post_rollback_pool == 0,
		"departing worker cleanup purges the blue-writer pool created before replacement");
	ok(runtime_definition_matches(admin, replacement_hgs, 0, 1, 150, 900) &&
		persistent_server_status(admin, original_hgs.green_reader, replacement.green_readers[0], "OFFLINE_SOFT") &&
		runtime_server_absent(admin, original_hgs.green_writer, replacement.green_writer) &&
		runtime_server_absent(admin, original_hgs.green_reader, replacement.green_readers[1]),
		"disabled runtime definition records the replacement hostgroups and changed green membership");
	ok(runtime_server_ssl(admin, original_hgs.green_reader, replacement.green_readers[0], 1),
		"active pre-completion green membership applies its use_ssl mutation before becoming offline");

	auto [replacement_seq_rc, replacement_seq] = sim.probe_log_last_sequence();
	rc = replacement_seq_rc == EXIT_SUCCESS ? sim.topology_update(replacement_backends,
		topology_with_readers(replacement_fresh, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	if (rc == EXIT_SUCCESS) rc = execute_all(admin, {
		"UPDATE mysql_aws_rds_bgd_hostgroups SET active=1 WHERE writer_hostgroup=1340",
		"LOAD MYSQL SERVERS TO RUNTIME",
	});
	auto replacement_chain = rc == EXIT_SUCCESS ? wait_for_replacement_probe_chain(admin, sim, replacement_seq,
		"replacement-input", replacement_fresh, replacement_hgs, 1) :
		Replacement_Probe_Chain { EXIT_FAILURE, {}, EXIT_FAILURE, {} };
	int replacement_status_rc = replacement_chain.table_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, replacement_seq,
		"replacement-input", replacement_hgs, "fresh replacement", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	int replacement_effects_rc = replacement_status_rc == EXIT_SUCCESS ? wait_for_writer_placement(admin, sim, replacement_seq,
		"replacement-input", replacement_hgs, replacement_fresh, "fresh replacement effects", true) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && replacement_chain.table_rc == EXIT_SUCCESS,
		"replacement starts from a fresh blue table-check probe");
	ok(replacement_chain.green_rc == EXIT_SUCCESS && replacement_chain.green.encrypted &&
		replacement_chain.green.backend.host == replacement_fresh.green_writer.ip,
		"replacement observes the fresh green writer metadata probe with TLS enabled");
	ok(replacement_status_rc == EXIT_SUCCESS,
		"replacement republishes the current in-progress phase");
	ok(replacement_effects_rc == EXIT_SUCCESS,
		"replacement applies in-progress writer demotion using the current definition");
	ok(runtime_server_ssl(admin, replacement_hgs.green_writer, replacement_fresh.green_writer, 1) &&
		runtime_server_ssl(admin, replacement_hgs.green_reader, replacement_fresh.green_readers[0], 1),
		"replacement runtime rows retain distinguishable TLS values for the fresh green membership");
	ok(runtime_definition_matches(admin, replacement_hgs, 1, 1, 150, 900) &&
		runtime_server_absent(admin, original_hgs.green_writer, replacement.green_writer) &&
		runtime_server_absent(admin, original_hgs.green_reader, replacement.green_readers[1]),
		"replacement state uses current hostgroups without restoring removed green membership");
	auto [stale_probe_rc, stale_probe] = bgd_wait_for_probe(sim, replacement_chain.green.sequence_id,
		replacement.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kNoReplacementTimeoutMs, -1, admin,
		"replacement-input", "stale green negative probe", replacement_hgs.blue_writer,
		{ replacement_hgs.blue_writer, replacement_hgs.blue_reader, replacement_hgs.green_writer, replacement_hgs.green_reader });
	ok(replacement_chain.green_rc == EXIT_SUCCESS && stale_probe_rc == ETIMEDOUT,
		"replacement does not probe the removed stale green writer endpoint");

	auto [delete_seq_rc, delete_seq] = sim.probe_log_last_sequence();
	rc = delete_seq_rc == EXIT_SUCCESS ? execute_all(admin, {
		"DELETE FROM mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=1340",
		"LOAD MYSQL SERVERS TO RUNTIME",
	}) : EXIT_FAILURE;
	int delete_cleanup_rc = rc == EXIT_SUCCESS ? wait_for_writer_placement(admin, sim, delete_seq,
		"replacement-input", replacement_hgs, replacement_fresh, "active row deletion", false) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && delete_cleanup_rc == EXIT_SUCCESS,
		"deleting the active BGD row performs phase-appropriate departing cleanup");
	ok(runtime_definition_absent(admin, replacement_hgs.blue_writer),
		"deleting the active BGD row removes its runtime worker definition");

	// Weight and comment are intentionally absent from the worker definition.  They must not
	// reset a metadata worker back through a fresh table check; TLS, which is an input, must.
	RDS_BGD_Cluster irrelevant = bgd_cluster_2_init();
	BGD_Hostgroups irrelevant_hgs { 1350, 1351, 1352, 1353 };
	vector<Endpoint> irrelevant_backends = scenario_backends(irrelevant);
	if (reset_scenario(admin, sim, irrelevant_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset irrelevant-input scenario");
	set_writers_writable(sim, irrelevant);
	auto [irrelevant_start_rc, irrelevant_start] = sim.probe_log_last_sequence();
	rc = irrelevant_start_rc == EXIT_SUCCESS ? sim.topology_update(irrelevant_backends,
		topology_with_readers(irrelevant, "AVAILABLE")) : EXIT_FAILURE;
	int irrelevant_setup_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, irrelevant, irrelevant_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ irrelevant.blue_writer, irrelevant.blue_readers[0], irrelevant.blue_readers[1] },
		{ irrelevant.green_writer, irrelevant.green_readers[0] }, 0, 0) : EXIT_FAILURE;
	int irrelevant_available_rc = irrelevant_setup_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, irrelevant_start,
		"irrelevant-input", irrelevant_hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	auto [irrelevant_ready_rc, irrelevant_ready] = irrelevant_available_rc == EXIT_SUCCESS ? bgd_wait_for_probe(sim,
		irrelevant_start, irrelevant.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin,
		"irrelevant-input", "available metadata", irrelevant_hgs.blue_writer,
		{ irrelevant_hgs.blue_writer, irrelevant_hgs.blue_reader, irrelevant_hgs.green_writer, irrelevant_hgs.green_reader }) :
		rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	if (irrelevant_ready_rc != EXIT_SUCCESS) BAIL_OUT("failed to establish irrelevant-input metadata baseline");
	rc = execute_all(admin, {
		"UPDATE mysql_servers SET weight=weight+7,comment='BGD TAP irrelevant replacement input' WHERE hostgroup_id=1350 AND hostname=" +
			bgd_sql_quote(irrelevant.blue_writer.hostname) + " AND port=3306",
		"LOAD MYSQL SERVERS TO RUNTIME",
	});
	auto [no_replace_rc, no_replace] = rc == EXIT_SUCCESS ? bgd_wait_for_probe_from_backends(sim, irrelevant_ready.sequence_id,
		{ irrelevant.blue_writer.endpoint(), irrelevant.green_writer.endpoint() }, RDS_BGD_Probe_Kind::table_check,
		kNoReplacementTimeoutMs) : rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	ok(rc == EXIT_SUCCESS && no_replace_rc == ETIMEDOUT,
		"irrelevant weight and comment changes do not restart the worker with a table-check probe");

	auto [tls_seq_rc, tls_seq] = sim.probe_log_last_sequence();
	rc = tls_seq_rc == EXIT_SUCCESS ? execute_all(admin, {
		"UPDATE mysql_servers SET use_ssl=1 WHERE hostgroup_id=1352 AND hostname=" +
			bgd_sql_quote(irrelevant.green_writer.hostname) + " AND port=3306",
		"LOAD MYSQL SERVERS TO RUNTIME",
	}) : EXIT_FAILURE;
	int tls_replacement_rc = rc == EXIT_SUCCESS ? wait_for_replacement_table(admin, sim, tls_seq,
		"irrelevant-input", irrelevant, irrelevant_hgs) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && tls_replacement_rc == EXIT_SUCCESS,
		"relevant TLS input change restarts probing with a fresh table check");
	ok(runtime_server_ssl(admin, irrelevant_hgs.green_writer, irrelevant.green_writer, 1),
		"relevant TLS input change is present in the replacement runtime server row");

	// A worker can also be replaced after AWS has already published writer completion.  The
	// replacement must take the accepted fresh completed path, not reuse its predecessor's map.
	RDS_BGD_Cluster completed = bgd_cluster_3_init();
	BGD_Hostgroups completed_hgs { 1360, 1361, 1362, 1363 };
	vector<Endpoint> completed_backends = scenario_backends(completed);
	if (reset_scenario(admin, sim, completed_backends) != EXIT_SUCCESS) BAIL_OUT("failed to reset completed replacement scenario");
	set_writers_writable(sim, completed);
	auto [completed_start_rc, completed_start] = sim.probe_log_last_sequence();
	rc = completed_start_rc == EXIT_SUCCESS ? sim.topology_update(completed_backends,
		topology_with_readers(completed, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	int completed_setup_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, completed, completed_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ completed.blue_writer, completed.blue_readers[0], completed.blue_readers[1] },
		{ completed.green_writer, completed.green_readers[0] }, 0, 0) : EXIT_FAILURE;
	int completed_progress_rc = completed_setup_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, completed_start,
		"completed-replacement", completed_hgs, "in-progress", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	auto [completed_publish_rc, completed_publish] = sim.probe_log_last_sequence();
	rc = completed_progress_rc == EXIT_SUCCESS && completed_publish_rc == EXIT_SUCCESS ?
		sim.topology_update(completed_backends, target_only_completed(completed)) : EXIT_FAILURE;
	int completed_status_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, completed_publish,
		"completed-replacement", completed_hgs, "published completed", "READER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(completed_setup_rc == EXIT_SUCCESS && completed_progress_rc == EXIT_SUCCESS && rc == EXIT_SUCCESS && completed_status_rc == EXIT_SUCCESS,
		"published SWITCHOVER_COMPLETED reaches the accepted reader-switchover phase");

	auto [completed_replace_seq_rc, completed_replace_seq] = sim.probe_log_last_sequence();
	rc = completed_replace_seq_rc == EXIT_SUCCESS ? execute_all(admin, {
		"UPDATE mysql_aws_rds_bgd_hostgroups SET check_timeout_ms=950 WHERE writer_hostgroup=1360",
		"LOAD MYSQL SERVERS TO RUNTIME",
	}) : EXIT_FAILURE;
	int completed_probe_rc = rc == EXIT_SUCCESS ? wait_for_replacement_table(admin, sim, completed_replace_seq,
		"completed-replacement", completed, completed_hgs) : EXIT_FAILURE;
	int completed_replacement_status_rc = completed_probe_rc == EXIT_SUCCESS ? wait_for_status(admin, sim,
		completed_replace_seq, "completed-replacement", completed_hgs, "fresh completed replacement",
		"READER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && completed_probe_rc == EXIT_SUCCESS,
		"completed-phase replacement starts from a fresh table-check probe");
	ok(completed_replacement_status_rc == EXIT_SUCCESS,
		"completed-phase replacement republishes the accepted reader-switchover behavior");

	if (reset_scenario(admin, sim, completed_backends) != EXIT_SUCCESS) diag("failed to clean worker replacement scenario");
	mysql_close(admin);
	return exit_status();
}
