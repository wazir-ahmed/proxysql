/**
 * @file test_rds_bgd_lifecycle-t.cpp
 * @brief Normal AWS RDS Blue/Green forward lifecycle coverage.
 *
 * Test flow:
 * 1. AVAILABLE establishes blue routing plus eligible green pools.
 * 2. SWITCHOVER_INITIATED suppresses deployment-member placement changes.
 * 3. SWITCHOVER_IN_PROGRESS demotes the mapped blue writer.
 * 4. SWITCHOVER_IN_POST_PROCESSING restores mapped placement and drains pools.
 * 5. Writer completion defers green cleanup until the reader signal.
 * 6. Empty topology completes cleanup, resumes blue probing, and reaches NONE.
 *
 * Repeated observations in every phase verify idempotent public behavior.
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

int wait_for_in_progress_effects(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs)
{
	const RDS_BGD_Host& writer = cluster.blue_writer;
	const RDS_BGD_Host& reader = cluster.blue_readers[0];
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status='WRITER_SWITCHOVER_IN_PROGRESS')=1 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(writer.hostname) + " AND port=" + to_string(writer.port) + ")=0 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(writer.hostname) + " AND port=" + to_string(writer.port) +
		" AND status='ONLINE')=1 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(reader.hostname) + " AND port=" + to_string(reader.port) +
		" AND status='ONLINE')=1 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(reader.hostname) + " AND port=" + to_string(reader.port) + ")=0";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, kScenario, "in-progress effects",
		"blue-writer demotion and mapped-reader placement", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

int wait_for_post_runtime_effects(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs)
{
	const RDS_BGD_Host& writer = cluster.blue_writer;
	const RDS_BGD_Host& mapped_reader = cluster.blue_readers[0];
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status='WRITER_SWITCHOVER_POST_PROCESSING')=1 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(writer.hostname) + " AND port=" + to_string(writer.port) +
		" AND status='ONLINE')=1 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(writer.hostname) + " AND port=" + to_string(writer.port) + ")=0 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(mapped_reader.hostname) + " AND port=" + to_string(mapped_reader.port) +
		" AND status='ONLINE')=1 AND " +
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(mapped_reader.hostname) + " AND port=" + to_string(mapped_reader.port) + ")=0";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, kScenario, "post-processing effects",
		"writer and mapped-reader placement", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

int wait_for_blue_writer_pool_drain(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs)
{
	const string query = "SELECT COALESCE(SUM(ConnUsed+ConnFree),0)=0 FROM stats_mysql_connection_pool WHERE srv_host=" +
		bgd_sql_quote(cluster.blue_writer.hostname);
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, kScenario, "post-processing pool drain",
		"mapped blue-writer connection pool drained", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
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

struct Lifecycle_Context {
	const CommandLine& cl;
	MYSQL* admin;
	RDS_BGD_Simulator& sim;
	RDS_BGD_Cluster cluster;
	BGD_Hostgroups hgs;
	vector<Endpoint> backends;

	Lifecycle_Context(const CommandLine& cl_, MYSQL* admin_, RDS_BGD_Simulator& sim_)
		: cl(cl_), admin(admin_), sim(sim_), cluster(bgd_cluster_init()),
		  hgs { 970, 971, 972, 973 }, backends(topology_backends(cluster)) {}
};

/**
 * Establish AVAILABLE state, create blue and green pools, and verify that a
 * repeated observation leaves writer placement unchanged.
 */
void test_available_phase(Lifecycle_Context& ctx) {
	// Publish AVAILABLE and wait for the worker's public runtime state.
	auto [available_seq_rc, available_seq] = ctx.sim.probe_log_last_sequence();
	if (available_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read AVAILABLE probe baseline");
	int rc = ctx.sim.topology_update(ctx.backends, topology_with_one_reader_pair(ctx.cluster, "AVAILABLE"));
	int available_rc = rc == EXIT_SUCCESS ?
		wait_for_status(ctx.admin, ctx.sim, available_seq, ctx.hgs, "available", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && available_rc == EXIT_SUCCESS,
		"recorded AVAILABLE observation enters AVAILABLE runtime status");

	// Establish pools in every eligible hostgroup before lifecycle effects begin.
	int blue_connect_rc = connect_and_echo(ctx.cl).first;
	int green_writer_pool_rc = set_default_hostgroup(ctx.admin, ctx.hgs.green_writer) == EXIT_SUCCESS ?
		connect_and_echo(ctx.cl).first : EXIT_FAILURE;
	int green_reader_pool_rc = set_default_hostgroup(ctx.admin, ctx.hgs.green_reader) == EXIT_SUCCESS ?
		connect_and_echo(ctx.cl).first : EXIT_FAILURE;
	int restore_blue_rc = set_default_hostgroup(ctx.admin, ctx.hgs.blue_writer);
	auto [blue_pool_count_rc, blue_pool] =
		bgd_connection_pool_count(ctx.admin, ctx.hgs.blue_writer, ctx.cluster.blue_writer.hostname);
	auto [green_writer_pool_count_rc, green_writer_pool] =
		bgd_connection_pool_count(ctx.admin, ctx.hgs.green_writer);
	auto [green_reader_pool_count_rc, green_reader_pool] =
		bgd_connection_pool_count(ctx.admin, ctx.hgs.green_reader);
	ok(blue_connect_rc == EXIT_SUCCESS && blue_pool_count_rc == EXIT_SUCCESS && blue_pool >= 1 &&
		green_writer_pool_rc == EXIT_SUCCESS && green_writer_pool_count_rc == EXIT_SUCCESS && green_writer_pool >= 1 &&
		green_reader_pool_rc == EXIT_SUCCESS && green_reader_pool_count_rc == EXIT_SUCCESS && green_reader_pool >= 1 &&
		restore_blue_rc == EXIT_SUCCESS,
		"AVAILABLE establishes blue and eligible green connection pools before lifecycle effects");

	// Repeat AVAILABLE to prove the phase is idempotent.
	auto [available_repeat_seq_rc, available_repeat_seq] = ctx.sim.probe_log_last_sequence();
	rc = available_repeat_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends, topology_with_one_reader_pair(ctx.cluster, "AVAILABLE")) : EXIT_FAILURE;
	int available_repeat_rc = rc == EXIT_SUCCESS ?
		wait_for_observation(ctx.admin, ctx.sim, available_repeat_seq, ctx.cluster, ctx.hgs, "available repeat") : EXIT_FAILURE;
	int available_repeat_status_rc = rc == EXIT_SUCCESS ?
		wait_for_status(ctx.admin, ctx.sim, available_repeat_seq, ctx.hgs, "available repeat", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && available_repeat_rc == EXIT_SUCCESS && available_repeat_status_rc == EXIT_SUCCESS &&
		writer_in_expected_placement(ctx.admin, ctx.hgs, ctx.cluster, true, false),
		"repeated AVAILABLE observation preserves status and writer placement");
}

/**
 * Enter SWITCHOVER_INITIATED and verify that read-only changes for deployment
 * members remain suppressed across a repeated observation.
 */
int64_t test_switchover_initiated_phase(Lifecycle_Context& ctx) {
	// Publish initiated and wait for the corresponding public runtime state.
	auto [initiated_seq_rc, initiated_seq] = ctx.sim.probe_log_last_sequence();
	int rc = initiated_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends, topology_with_one_reader_pair(ctx.cluster, "SWITCHOVER_INITIATED")) :
		EXIT_FAILURE;
	int initiated_rc = rc == EXIT_SUCCESS ?
		wait_for_status(ctx.admin, ctx.sim, initiated_seq, ctx.hgs, "initiated", "WRITER_SWITCHOVER_INITIATED") :
		EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_rc == EXIT_SUCCESS,
		"recorded SWITCHOVER_INITIATED observation enters initiated runtime status");

	// Flip simulated roles and verify that suppression prevents placement actions.
	int64_t initiated_writer_log = last_read_only_log_time(ctx.admin, ctx.cluster.blue_writer);
	int64_t initiated_reader_log = last_read_only_log_time(ctx.admin, ctx.cluster.blue_readers[0]);
	set_read_only(ctx.sim, ctx.cluster.blue_writer, true);
	set_read_only(ctx.sim, ctx.cluster.blue_readers[0], false);
	auto [initiated_suppression_seq_rc, initiated_suppression_seq] = ctx.sim.probe_log_last_sequence();
	int initiated_suppression_rc = initiated_suppression_seq_rc == EXIT_SUCCESS ?
		wait_for_observation(ctx.admin, ctx.sim, initiated_suppression_seq, ctx.cluster, ctx.hgs,
			"initiated suppression") : EXIT_FAILURE;
	ok(initiated_suppression_rc == EXIT_SUCCESS &&
		writer_in_expected_placement(ctx.admin, ctx.hgs, ctx.cluster, true, false) &&
		server_has_status(ctx.admin, ctx.hgs.blue_reader, ctx.cluster.blue_readers[0], "ONLINE") &&
		server_absent(ctx.admin, ctx.hgs.blue_writer, ctx.cluster.blue_readers[0]) &&
		no_read_only_log_after(ctx.admin, ctx.cluster.blue_writer, initiated_writer_log) &&
		no_read_only_log_after(ctx.admin, ctx.cluster.blue_readers[0], initiated_reader_log),
		"initiated suppresses read-only placement changes for deployment members");

	// Repeat initiated to verify stable status and suppressed placement.
	auto [initiated_repeat_seq_rc, initiated_repeat_seq] = ctx.sim.probe_log_last_sequence();
	rc = initiated_repeat_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends, topology_with_one_reader_pair(ctx.cluster, "SWITCHOVER_INITIATED")) :
		EXIT_FAILURE;
	int initiated_repeat_rc = rc == EXIT_SUCCESS ?
		wait_for_observation(ctx.admin, ctx.sim, initiated_repeat_seq, ctx.cluster, ctx.hgs, "initiated repeat") :
		EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && initiated_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(ctx.admin, ctx.sim, initiated_repeat_seq, ctx.hgs, "initiated repeat",
			"WRITER_SWITCHOVER_INITIATED") == EXIT_SUCCESS &&
		writer_in_expected_placement(ctx.admin, ctx.hgs, ctx.cluster, true, false),
		"repeated initiated observation preserves status and suppressed placement");

	return initiated_reader_log;
}

/**
 * Enter SWITCHOVER_IN_PROGRESS, verify mapped-writer demotion and continued
 * reader suppression, then repeat the observation.
 */
void test_switchover_in_progress_phase(Lifecycle_Context& ctx, int64_t initiated_reader_log) {
	// Publish in-progress and wait for mapped writer demotion.
	auto [progress_seq_rc, progress_seq] = ctx.sim.probe_log_last_sequence();
	int rc = progress_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends, topology_with_one_reader_pair(ctx.cluster, "SWITCHOVER_IN_PROGRESS")) :
		EXIT_FAILURE;
	int progress_rc = rc == EXIT_SUCCESS ?
		wait_for_status(ctx.admin, ctx.sim, progress_seq, ctx.hgs, "in progress",
			"WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && progress_rc == EXIT_SUCCESS,
		"recorded SWITCHOVER_IN_PROGRESS observation enters in-progress runtime status");

	int progress_effects_rc = progress_rc == EXIT_SUCCESS ?
		wait_for_in_progress_effects(ctx.admin, ctx.sim, progress_seq, ctx.cluster, ctx.hgs) : EXIT_FAILURE;
	ok(progress_effects_rc == EXIT_SUCCESS,
		"in-progress policy demotes the mapped blue writer while suppression keeps the blue reader placed");

	// Verify the pending reader promotion remains suppressed.
	auto [progress_suppression_seq_rc, progress_suppression_seq] = ctx.sim.probe_log_last_sequence();
	int progress_suppression_rc = progress_suppression_seq_rc == EXIT_SUCCESS ?
		wait_for_observation(ctx.admin, ctx.sim, progress_suppression_seq, ctx.cluster, ctx.hgs,
			"in-progress suppression") : EXIT_FAILURE;
	ok(progress_suppression_rc == EXIT_SUCCESS &&
		server_has_status(ctx.admin, ctx.hgs.blue_reader, ctx.cluster.blue_readers[0], "ONLINE") &&
		server_absent(ctx.admin, ctx.hgs.blue_writer, ctx.cluster.blue_readers[0]) &&
		no_read_only_log_after(ctx.admin, ctx.cluster.blue_readers[0], initiated_reader_log),
		"in-progress continues to suppress the pending read-only promotion");

	// Repeat in-progress to verify the demoted placement remains stable.
	auto [progress_repeat_seq_rc, progress_repeat_seq] = ctx.sim.probe_log_last_sequence();
	rc = progress_repeat_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends, topology_with_one_reader_pair(ctx.cluster, "SWITCHOVER_IN_PROGRESS")) :
		EXIT_FAILURE;
	int progress_repeat_rc = rc == EXIT_SUCCESS ?
		wait_for_observation(ctx.admin, ctx.sim, progress_repeat_seq, ctx.cluster, ctx.hgs, "in-progress repeat") :
		EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && progress_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(ctx.admin, ctx.sim, progress_repeat_seq, ctx.hgs, "in-progress repeat",
			"WRITER_SWITCHOVER_IN_PROGRESS") == EXIT_SUCCESS &&
		writer_in_expected_placement(ctx.admin, ctx.hgs, ctx.cluster, false, true),
		"repeated in-progress observation preserves status and blue-writer demotion");
}

/**
 * Enter post-processing, verify mapped placement and pool draining, then prove
 * that suppression and placement remain stable on repeat.
 */
void test_post_processing_phase(Lifecycle_Context& ctx, int64_t initiated_reader_log) {
	// Publish post-processing and wait for mapped runtime placement.
	auto [post_seq_rc, post_seq] = ctx.sim.probe_log_last_sequence();
	int rc = post_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends,
			topology_with_one_reader_pair(ctx.cluster, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	int post_rc = rc == EXIT_SUCCESS ?
		wait_for_status(ctx.admin, ctx.sim, post_seq, ctx.hgs, "post processing",
			"WRITER_SWITCHOVER_POST_PROCESSING") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && post_rc == EXIT_SUCCESS,
		"recorded SWITCHOVER_IN_POST_PROCESSING observation enters post-processing runtime status");

	int post_runtime_effects_rc = post_rc == EXIT_SUCCESS ?
		wait_for_post_runtime_effects(ctx.admin, ctx.sim, post_seq, ctx.cluster, ctx.hgs) : EXIT_FAILURE;
	ok(post_runtime_effects_rc == EXIT_SUCCESS,
		"post-processing restores writer placement and retains the mapped reader");

	// Drain the old blue-writer pool and verify new blue-hostgroup traffic reaches green.
	int post_pool_drain_rc = post_runtime_effects_rc == EXIT_SUCCESS ?
		wait_for_blue_writer_pool_drain(ctx.admin, ctx.sim, post_seq, ctx.cluster, ctx.hgs) : EXIT_FAILURE;
	ok(post_pool_drain_rc == EXIT_SUCCESS,
		"post-processing drains existing pools for the mapped blue writer hostname");

	auto [post_echo_rc, post_echo] = connect_and_echo(ctx.cl);
	ok(post_echo_rc == EXIT_SUCCESS && post_echo.find(ctx.cluster.green_writer.ip) != string::npos,
		"post-processing pins a new mapped-blue writer connection to the green backend IP");

	// Verify the mapped reader remains suppressed and repeat the phase.
	auto [post_suppression_seq_rc, post_suppression_seq] = ctx.sim.probe_log_last_sequence();
	int post_suppression_rc = post_suppression_seq_rc == EXIT_SUCCESS ?
		wait_for_observation(ctx.admin, ctx.sim, post_suppression_seq, ctx.cluster, ctx.hgs,
			"post-processing suppression") : EXIT_FAILURE;
	ok(post_suppression_rc == EXIT_SUCCESS &&
		server_has_status(ctx.admin, ctx.hgs.blue_reader, ctx.cluster.blue_readers[0], "ONLINE") &&
		server_absent(ctx.admin, ctx.hgs.blue_writer, ctx.cluster.blue_readers[0]) &&
		no_read_only_log_after(ctx.admin, ctx.cluster.blue_readers[0], initiated_reader_log),
		"post-processing continues to suppress read-only placement changes for the mapped reader");

	auto [post_repeat_seq_rc, post_repeat_seq] = ctx.sim.probe_log_last_sequence();
	rc = post_repeat_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends,
			topology_with_one_reader_pair(ctx.cluster, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	int post_repeat_rc = rc == EXIT_SUCCESS ?
		wait_for_observation(ctx.admin, ctx.sim, post_repeat_seq, ctx.cluster, ctx.hgs, "post-processing repeat") :
		EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && post_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(ctx.admin, ctx.sim, post_repeat_seq, ctx.hgs, "post-processing repeat",
			"WRITER_SWITCHOVER_POST_PROCESSING") == EXIT_SUCCESS &&
		writer_in_expected_placement(ctx.admin, ctx.hgs, ctx.cluster, true, false) &&
		server_has_status(ctx.admin, ctx.hgs.blue_reader, ctx.cluster.blue_readers[0], "ONLINE"),
		"repeated post-processing observation preserves status and mapped placement");
}

/**
 * Infer reader switchover from target-only completion, then process empty
 * topology and verify final probe, pool, and configured-row behavior.
 */
void test_reader_completion_phase(Lifecycle_Context& ctx) {
	// Publish target-only completion and defer green cleanup during reader switchover.
	auto [completed_seq_rc, completed_seq] = ctx.sim.probe_log_last_sequence();
	int rc = completed_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends, target_only_completed(ctx.cluster)) : EXIT_FAILURE;
	int completed_rc = rc == EXIT_SUCCESS ?
		wait_for_status(ctx.admin, ctx.sim, completed_seq, ctx.hgs, "target-only completed",
			"READER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && completed_rc == EXIT_SUCCESS,
		"target-only SWITCHOVER_COMPLETED observation enters inferred reader-switchover status");

	ok(green_rows_remain_online(ctx.admin, ctx.hgs, ctx.cluster),
		"writer completion defers green-row cleanup until the reader signal");

	auto [completed_repeat_seq_rc, completed_repeat_seq] = ctx.sim.probe_log_last_sequence();
	rc = completed_repeat_seq_rc == EXIT_SUCCESS ?
		ctx.sim.topology_update(ctx.backends, target_only_completed(ctx.cluster)) : EXIT_FAILURE;
	int completed_repeat_rc = rc == EXIT_SUCCESS ?
		wait_for_observation(ctx.admin, ctx.sim, completed_repeat_seq, ctx.cluster, ctx.hgs,
			"target-only completed repeat") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && completed_repeat_rc == EXIT_SUCCESS &&
		wait_for_status(ctx.admin, ctx.sim, completed_repeat_seq, ctx.hgs, "target-only completed repeat",
			"READER_SWITCHOVER_IN_PROGRESS") == EXIT_SUCCESS &&
		green_rows_remain_online(ctx.admin, ctx.hgs, ctx.cluster),
		"repeated target-only completed observation preserves reader-switchover phase and green rows");

	// Delete all topology rows to complete reader cleanup and resume blue probing.
	auto [empty_seq_rc, empty_seq] = ctx.sim.probe_log_last_sequence();
	rc = empty_seq_rc == EXIT_SUCCESS ? ctx.sim.topology_delete(ctx.backends) : EXIT_FAILURE;
	int empty_rc = rc == EXIT_SUCCESS ?
		wait_for_status(ctx.admin, ctx.sim, empty_seq, ctx.hgs, "present empty", "NONE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && empty_rc == EXIT_SUCCESS,
		"present-but-empty topology completes reader cleanup and reaches NONE");

	ok(server_has_status(ctx.admin, ctx.hgs.blue_reader, ctx.cluster.blue_readers[1], "ONLINE"),
		"final cleanup restores the unmatched blue reader to ONLINE");

	auto [empty_green_rc, empty_green_probe] = bgd_wait_for_probe(ctx.sim, empty_seq,
		ctx.cluster.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, ctx.admin,
		kScenario, "present-empty green observation", ctx.hgs.blue_writer,
		{ ctx.hgs.blue_writer, ctx.hgs.blue_reader, ctx.hgs.green_writer, ctx.hgs.green_reader });
	const uint64_t blue_probe_baseline = empty_green_rc == EXIT_SUCCESS ? empty_green_probe.sequence_id : empty_seq;
	auto [blue_after_empty_rc, blue_after_empty_probe] = bgd_wait_for_probe(ctx.sim, blue_probe_baseline,
		ctx.cluster.blue_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, ctx.admin, kScenario,
		"post-cleanup blue probe", ctx.hgs.blue_writer,
		{ ctx.hgs.blue_writer, ctx.hgs.blue_reader, ctx.hgs.green_writer, ctx.hgs.green_reader });
	ok(empty_green_rc == EXIT_SUCCESS && blue_after_empty_rc == EXIT_SUCCESS &&
		empty_green_probe.sequence_id < blue_after_empty_probe.sequence_id,
		"final cleanup removes the direct green probe pin and resumes blue-IP metadata probing");

	// Verify cleanup drains green pools without deleting configured green rows.
	auto [final_green_writer_pool_rc, final_green_writer_pool] =
		bgd_connection_pool_count(ctx.admin, ctx.hgs.green_writer);
	auto [final_green_reader_pool_rc, final_green_reader_pool] =
		bgd_connection_pool_count(ctx.admin, ctx.hgs.green_reader);
	ok(final_green_writer_pool_rc == EXIT_SUCCESS && final_green_writer_pool == 0 &&
		final_green_reader_pool_rc == EXIT_SUCCESS && final_green_reader_pool == 0,
		"final cleanup drains eligible green hostgroup pools");

	ok(green_rows_remain_online(ctx.admin, ctx.hgs, ctx.cluster),
		"final cleanup retains all configured green rows and statuses");
}

/**
 * Configure one deployment and execute the complete accepted forward lifecycle
 * through writer and reader switchover cleanup.
 */
void test_forward_lifecycle(const CommandLine& cl, MYSQL* admin, RDS_BGD_Simulator& sim) {
	Lifecycle_Context ctx { cl, admin, sim };

	// Reset both simulator and Admin state before configuring role and membership data.
	if (scenario_cleanup(admin, sim, ctx.backends) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset normal lifecycle scenario");
	}

	set_read_only(sim, ctx.cluster.blue_writer, false);
	set_read_only(sim, ctx.cluster.green_writer, false);
	set_read_only(sim, ctx.cluster.blue_readers[0], true);
	set_read_only(sim, ctx.cluster.blue_readers[1], true);
	set_read_only(sim, ctx.cluster.green_readers[0], true);
	set_read_only(sim, ctx.cluster.green_readers[1], true);

	if (bgd_admin_setup(admin, ctx.cluster, ctx.hgs, BGD_Admin_Mode::explicit_configuration,
		{ ctx.cluster.blue_writer, ctx.cluster.blue_readers[0], ctx.cluster.blue_readers[1] },
		{ ctx.cluster.green_writer, ctx.cluster.green_readers[0] }) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure normal lifecycle scenario");
	}

	test_available_phase(ctx);
	int64_t initiated_reader_log = test_switchover_initiated_phase(ctx);
	test_switchover_in_progress_phase(ctx, initiated_reader_log);
	test_post_processing_phase(ctx, initiated_reader_log);
	test_reader_completion_phase(ctx);
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
	if (bgd_register_test_cleanup(admin, sim) != EXIT_SUCCESS) BAIL_OUT("failed to register BGD TAP cleanup");

	test_forward_lifecycle(cl, admin, sim);

	int cleanup_rc = bgd_finish_test_cleanup(admin, sim);
	if (cleanup_rc != EXIT_SUCCESS) BAIL_OUT("failed to clean final lifecycle TAP state");
	mysql_close(admin);
	return exit_status();
}
