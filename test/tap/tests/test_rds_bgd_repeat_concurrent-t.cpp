/**
 * @file test_rds_bgd_repeat_concurrent-t.cpp
 * @brief Repeated AWS RDS Blue/Green lifecycle and concurrent-worker isolation coverage.
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
const uint32_t kNegativeProbeTimeoutMs = 500;

vector<Endpoint> topology_backends(RDS_BGD_Cluster& cluster) {
	return { cluster.blue_writer.endpoint(), cluster.green_writer.endpoint() };
}

vector<RDS_BGD_Topology_Row> topology_with_reader_pair(
	RDS_BGD_Cluster& cluster, const string& status)
{
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

vector<int> all_hostgroups(const BGD_Hostgroups& hgs) {
	return { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader };
}

void set_read_only(RDS_BGD_Simulator& sim, RDS_BGD_Host& host, bool value) {
	if (sim.read_only_update(host.host_endpoint(), value) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure simulated read_only state");
	}
}

void set_role_states(RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster) {
	set_read_only(sim, cluster.blue_writer, false);
	set_read_only(sim, cluster.green_writer, false);
	for (RDS_BGD_Host& reader : cluster.blue_readers) set_read_only(sim, reader, true);
	for (RDS_BGD_Host& reader : cluster.green_readers) set_read_only(sim, reader, true);
}

int reset_scenario(MYSQL* admin, RDS_BGD_Simulator& sim, const vector<Endpoint>& backends) {
	return bgd_admin_cleanup(admin) == EXIT_SUCCESS && sim.topology_drop(backends) == EXIT_SUCCESS ?
		EXIT_SUCCESS : EXIT_FAILURE;
}

int wait_for_status(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, const string& phase, const string& status)
{
	return bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status=" + bgd_sql_quote(status),
		kTimeoutSeconds, sim, sequence, scenario, phase, "runtime BGD status " + status,
		hgs.blue_writer, all_hostgroups(hgs));
}

int wait_for_writer_placement(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster,
	const string& phase, bool demoted)
{
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) +
		" AND port=3306 AND status='ONLINE')=" + string(demoted ? "0" : "1") + " AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) +
		" AND port=3306 AND status='ONLINE')=" + string(demoted ? "1" : "0") + " AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[0].hostname) +
		" AND port=3306 AND status='ONLINE')=1";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, phase,
		demoted ? "blue writer demoted into reader hostgroup" : "blue writer in writer hostgroup",
		hgs.blue_writer, all_hostgroups(hgs));
}

int wait_for_post_effects(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster,
	const string& phase)
{
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status='WRITER_SWITCHOVER_POST_PROCESSING')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) +
		" AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=0";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, phase,
		"blue writer restored to its configured writer hostgroup",
		hgs.blue_writer, all_hostgroups(hgs));
}

int wait_for_reset_effects(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster)
{
	const string query = "SELECT "
		"(SELECT COUNT(*) FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status='NONE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_writer) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) +
		" AND port=3306 AND status='ONLINE')=1 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_writer.hostname) + " AND port=3306)=0 AND "
		"(SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hgs.blue_reader) +
		" AND hostname=" + bgd_sql_quote(cluster.blue_readers[1].hostname) +
		" AND port=3306 AND status='ONLINE')=1";
	return bgd_wait_for_condition(admin, query, kTimeoutSeconds, sim, sequence, scenario, "present empty",
		"NONE with baseline blue placement restored", hgs.blue_writer, all_hostgroups(hgs));
}

int wait_for_pool_drain(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	const string& scenario, const string& phase, const BGD_Hostgroups& hgs,
	int hostgroup, const string& hostname)
{
	return bgd_wait_for_condition(admin,
		"SELECT COALESCE(SUM(ConnUsed+ConnFree),0)=0 FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hostgroup) + " AND srv_host=" + bgd_sql_quote(hostname),
		kTimeoutSeconds, sim, sequence, scenario, phase, "connection pool drained",
		hgs.blue_writer, all_hostgroups(hgs));
}

bool status_is(MYSQL* admin, const BGD_Hostgroups& hgs, const string& status) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status=" + bgd_sql_quote(status));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "1";
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

bool server_has_ssl(MYSQL* admin, int hostgroup, const RDS_BGD_Host& host, int use_ssl) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT use_ssl FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(hostgroup) +
		" AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=3306");
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 &&
		rows[0][0] == to_string(use_ssl);
}

bool writer_placement(MYSQL* admin, const BGD_Hostgroups& hgs,
	RDS_BGD_Cluster& cluster, bool demoted)
{
	return (demoted ? server_absent(admin, hgs.blue_writer, cluster.blue_writer) :
		server_has_status(admin, hgs.blue_writer, cluster.blue_writer, "ONLINE")) &&
		(demoted ? server_has_status(admin, hgs.blue_reader, cluster.blue_writer, "ONLINE") :
		server_absent(admin, hgs.blue_reader, cluster.blue_writer));
}

bool post_effects(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster) {
	return status_is(admin, hgs, "WRITER_SWITCHOVER_POST_PROCESSING") &&
		writer_placement(admin, hgs, cluster, false);
}

bool green_membership_is(MYSQL* admin, const BGD_Hostgroups& hgs,
	RDS_BGD_Cluster& present, RDS_BGD_Cluster& absent, int use_ssl)
{
	return server_has_ssl(admin, hgs.green_writer, present.green_writer, use_ssl) &&
		server_has_ssl(admin, hgs.green_reader, present.green_readers[0], use_ssl) &&
		server_absent(admin, hgs.green_writer, absent.green_writer) &&
		server_absent(admin, hgs.green_reader, absent.green_readers[0]);
}

int replace_green_membership(MYSQL* admin, const BGD_Hostgroups& hgs,
	RDS_BGD_Cluster& old_deployment, RDS_BGD_Cluster& new_deployment, int use_ssl)
{
	if (execute_all(admin, {
		"DELETE FROM mysql_servers WHERE hostgroup_id=" + to_string(hgs.green_writer) +
			" AND hostname=" + bgd_sql_quote(old_deployment.green_writer.hostname) + " AND port=3306",
		"DELETE FROM mysql_servers WHERE hostgroup_id=" + to_string(hgs.green_reader) +
			" AND hostname=" + bgd_sql_quote(old_deployment.green_readers[0].hostname) + " AND port=3306",
	}) != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, new_deployment, hgs,
			{ new_deployment.green_writer, new_deployment.green_readers[0] }, true, use_ssl) != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	return execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" });
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

struct Lifecycle_Pools {
	int rc;
	string blue_echo;
	string green_writer_echo;
	string green_reader_echo;
	int64_t blue_count;
	int64_t green_writer_count;
	int64_t green_reader_count;
};

Lifecycle_Pools establish_lifecycle_pools(
	const CommandLine& cl, MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster)
{
	int rc = set_default_hostgroup(admin, hgs.blue_writer);
	auto [blue_rc, blue_echo] = rc == EXIT_SUCCESS ?
		connect_and_echo(cl) : rc_t<string> { EXIT_FAILURE, {} };
	if (blue_rc != EXIT_SUCCESS) rc = EXIT_FAILURE;
	if (rc == EXIT_SUCCESS) rc = set_default_hostgroup(admin, hgs.green_writer);
	auto [green_writer_rc, green_writer_echo] = rc == EXIT_SUCCESS ?
		connect_and_echo(cl) : rc_t<string> { EXIT_FAILURE, {} };
	if (green_writer_rc != EXIT_SUCCESS) rc = EXIT_FAILURE;
	if (rc == EXIT_SUCCESS) rc = set_default_hostgroup(admin, hgs.green_reader);
	auto [green_reader_rc, green_reader_echo] = rc == EXIT_SUCCESS ?
		connect_and_echo(cl) : rc_t<string> { EXIT_FAILURE, {} };
	if (green_reader_rc != EXIT_SUCCESS) rc = EXIT_FAILURE;
	if (set_default_hostgroup(admin, hgs.blue_writer) != EXIT_SUCCESS) rc = EXIT_FAILURE;

	auto [blue_count_rc, blue_count] =
		bgd_connection_pool_count(admin, hgs.blue_writer, cluster.blue_writer.hostname);
	auto [green_writer_count_rc, green_writer_count] =
		bgd_connection_pool_count(admin, hgs.green_writer, cluster.green_writer.hostname);
	auto [green_reader_count_rc, green_reader_count] =
		bgd_connection_pool_count(admin, hgs.green_reader, cluster.green_readers[0].hostname);
	if (blue_count_rc != EXIT_SUCCESS || green_writer_count_rc != EXIT_SUCCESS ||
		green_reader_count_rc != EXIT_SUCCESS) rc = EXIT_FAILURE;
	return { rc, blue_echo, green_writer_echo, green_reader_echo,
		blue_count, green_writer_count, green_reader_count };
}

bool lifecycle_pools_match(const Lifecycle_Pools& pools, RDS_BGD_Cluster& cluster) {
	return pools.rc == EXIT_SUCCESS &&
		pools.blue_echo.find(cluster.blue_writer.ip) != string::npos &&
		pools.green_writer_echo.find(cluster.green_writer.ip) != string::npos &&
		pools.green_reader_echo.find(cluster.green_readers[0].ip) != string::npos &&
		pools.blue_count >= 1 && pools.green_writer_count >= 1 && pools.green_reader_count >= 1;
}

bool green_pools_match(const Lifecycle_Pools& pools, RDS_BGD_Cluster& cluster) {
	return pools.rc == EXIT_SUCCESS &&
		pools.green_writer_echo.find(cluster.green_writer.ip) != string::npos &&
		pools.green_reader_echo.find(cluster.green_readers[0].ip) != string::npos &&
		pools.green_writer_count >= 1 && pools.green_reader_count >= 1;
}

bool green_pools_are_zero(MYSQL* admin, const BGD_Hostgroups& hgs, RDS_BGD_Cluster& cluster) {
	auto [writer_rc, writer_count] =
		bgd_connection_pool_count(admin, hgs.green_writer, cluster.green_writer.hostname);
	auto [reader_rc, reader_count] =
		bgd_connection_pool_count(admin, hgs.green_reader, cluster.green_readers[0].hostname);
	return writer_rc == EXIT_SUCCESS && writer_count == 0 &&
		reader_rc == EXIT_SUCCESS && reader_count == 0;
}

}  // namespace

int main() {
	plan(38);

	CommandLine cl {};
	if (cl.getEnv()) BAIL_OUT("failed to load TAP environment");
	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) BAIL_OUT("failed to connect to ProxySQL Admin");
	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}

	const string repeat_scenario = "repeat-deployment";
	RDS_BGD_Cluster deployment_a = bgd_cluster_init();
	RDS_BGD_Cluster deployment_b = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups repeat_hgs { 1400, 1401, 1402, 1403 };
	vector<Endpoint> repeat_backends {
		deployment_a.blue_writer.endpoint(),
		deployment_a.green_writer.endpoint(),
		deployment_b.green_writer.endpoint(),
	};
	if (reset_scenario(admin, sim, repeat_backends) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset repeated-lifecycle scenario");
	}
	set_role_states(sim, deployment_a);
	set_role_states(sim, deployment_b);
	if (bgd_admin_setup(admin, deployment_a, repeat_hgs, BGD_Admin_Mode::explicit_configuration,
		{ deployment_a.blue_writer, deployment_a.blue_readers[0], deployment_a.blue_readers[1] },
		{ deployment_a.green_writer, deployment_a.green_readers[0] }, 0, 0) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure deployment A");
	}

	auto [a_available_seq_rc, a_available_seq] = sim.probe_log_last_sequence();
	int rc = a_available_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_a), topology_with_reader_pair(deployment_a, "AVAILABLE")) : EXIT_FAILURE;
	int phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, a_available_seq,
		repeat_scenario, repeat_hgs, "deployment A available", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS,
		"deployment A records AVAILABLE before its first lifecycle");

	Lifecycle_Pools a_pools = phase_rc == EXIT_SUCCESS ?
		establish_lifecycle_pools(cl, admin, repeat_hgs, deployment_a) :
		Lifecycle_Pools { EXIT_FAILURE, {}, {}, {}, 0, 0, 0 };
	ok(lifecycle_pools_match(a_pools, deployment_a),
		"deployment A establishes blue and green destination pools");

	auto [a_initiated_seq_rc, a_initiated_seq] = sim.probe_log_last_sequence();
	rc = a_initiated_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_a), topology_with_reader_pair(deployment_a, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, a_initiated_seq,
		repeat_scenario, repeat_hgs, "deployment A initiated", "WRITER_SWITCHOVER_INITIATED") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS &&
		writer_placement(admin, repeat_hgs, deployment_a, false),
		"deployment A records initiated while retaining blue-writer placement");

	auto [a_progress_seq_rc, a_progress_seq] = sim.probe_log_last_sequence();
	rc = a_progress_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_a), topology_with_reader_pair(deployment_a, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, a_progress_seq,
		repeat_scenario, repeat_hgs, "deployment A in progress", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	int effects_rc = phase_rc == EXIT_SUCCESS ? wait_for_writer_placement(admin, sim, a_progress_seq,
		repeat_scenario, repeat_hgs, deployment_a, "deployment A in-progress effects", true) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"deployment A in-progress policy demotes its blue writer");

	auto [a_post_seq_rc, a_post_seq] = sim.probe_log_last_sequence();
	rc = a_post_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_a),
		topology_with_reader_pair(deployment_a, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	effects_rc = rc == EXIT_SUCCESS ? wait_for_post_effects(admin, sim, a_post_seq,
		repeat_scenario, repeat_hgs, deployment_a, "deployment A post-processing") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"deployment A post-processing restores its blue-writer placement");

	int pool_rc = effects_rc == EXIT_SUCCESS ? wait_for_pool_drain(admin, sim, a_post_seq,
		repeat_scenario, "deployment A post-processing", repeat_hgs,
		repeat_hgs.blue_writer, deployment_a.blue_writer.hostname) : EXIT_FAILURE;
	ok(pool_rc == EXIT_SUCCESS,
		"deployment A post-processing drains the causal blue-writer pool");

	auto [a_route_rc, a_route] = pool_rc == EXIT_SUCCESS ?
		connect_and_echo(cl) : rc_t<string> { EXIT_FAILURE, {} };
	ok(a_route_rc == EXIT_SUCCESS && a_route.find(deployment_a.green_writer.ip) != string::npos,
		"deployment A post-processing routes the mapped blue destination to deployment A green");

	auto [a_completed_seq_rc, a_completed_seq] = sim.probe_log_last_sequence();
	rc = a_completed_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_a), target_only_completed(deployment_a)) : EXIT_FAILURE;
	phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, a_completed_seq,
		repeat_scenario, repeat_hgs, "deployment A completed", "READER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS,
		"deployment A completion enters inferred reader switchover");

	auto [a_empty_seq_rc, a_empty_seq] = sim.probe_log_last_sequence();
	rc = a_empty_seq_rc == EXIT_SUCCESS ? sim.topology_delete(topology_backends(deployment_a)) : EXIT_FAILURE;
	effects_rc = rc == EXIT_SUCCESS ? wait_for_reset_effects(admin, sim, a_empty_seq,
		repeat_scenario, repeat_hgs, deployment_a) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"deployment A present-empty topology resets the FSM and restores baseline placement");

	ok(green_pools_are_zero(admin, repeat_hgs, deployment_a),
		"deployment A reset drains its green destination pools");

	auto [a_empty_green_rc, a_empty_green] = bgd_wait_for_probe(sim, a_empty_seq,
		deployment_a.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0,
		admin, repeat_scenario, "deployment A reset green probe", repeat_hgs.blue_writer,
		all_hostgroups(repeat_hgs));
	const uint64_t a_blue_baseline =
		a_empty_green_rc == EXIT_SUCCESS ? a_empty_green.sequence_id : a_empty_seq;
	auto [a_empty_blue_rc, a_empty_blue] = bgd_wait_for_probe(sim, a_blue_baseline,
		deployment_a.blue_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0,
		admin, repeat_scenario, "deployment A reset blue probe", repeat_hgs.blue_writer,
		all_hostgroups(repeat_hgs));
	ok(a_empty_green_rc == EXIT_SUCCESS && a_empty_blue_rc == EXIT_SUCCESS &&
		a_empty_green.sequence_id < a_empty_blue.sequence_id,
		"deployment A reset removes the green pin and resumes blue metadata probing");

	auto [b_available_seq_rc, b_available_seq] = sim.probe_log_last_sequence();
	rc = b_available_seq_rc == EXIT_SUCCESS ? replace_green_membership(
		admin, repeat_hgs, deployment_a, deployment_b, 1) : EXIT_FAILURE;
	if (rc == EXIT_SUCCESS) {
		rc = sim.topology_update(
			topology_backends(deployment_b), topology_with_reader_pair(deployment_b, "AVAILABLE"));
	}
	auto [b_probe_rc, b_probe] = rc == EXIT_SUCCESS ? bgd_wait_for_probe(sim, b_available_seq,
		deployment_b.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 1,
		admin, repeat_scenario, "deployment B available probe", repeat_hgs.blue_writer,
		all_hostgroups(repeat_hgs)) : rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	phase_rc = b_probe_rc == EXIT_SUCCESS ? wait_for_status(admin, sim, b_available_seq,
		repeat_scenario, repeat_hgs, "deployment B available", "AVAILABLE") : EXIT_FAILURE;
	ok(green_membership_is(admin, repeat_hgs, deployment_b, deployment_a, 1),
		"second lifecycle runtime rows contain only TLS-enabled deployment B green membership");
	ok(rc == EXIT_SUCCESS && b_probe_rc == EXIT_SUCCESS && b_probe.encrypted &&
		phase_rc == EXIT_SUCCESS,
		"second lifecycle records AVAILABLE through the deployment B TLS destination");

	auto [stale_a_probe_rc, stale_a_probe] = bgd_wait_for_probe(sim, b_probe.sequence_id,
		deployment_a.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kNegativeProbeTimeoutMs, -1,
		admin, repeat_scenario, "stale deployment A negative probe", repeat_hgs.blue_writer,
		all_hostgroups(repeat_hgs));
	ok(b_probe_rc == EXIT_SUCCESS && stale_a_probe_rc == ETIMEDOUT,
		"second lifecycle does not resume probes to the removed deployment A destination");

	Lifecycle_Pools b_pools = phase_rc == EXIT_SUCCESS ?
		establish_lifecycle_pools(cl, admin, repeat_hgs, deployment_b) :
		Lifecycle_Pools { EXIT_FAILURE, {}, {}, {}, 0, 0, 0 };
	if (!green_pools_match(b_pools, deployment_b) ||
		!green_pools_are_zero(admin, repeat_hgs, deployment_a)) {
		auto [old_writer_pool_rc, old_writer_pool] = bgd_connection_pool_count(
			admin, repeat_hgs.green_writer, deployment_a.green_writer.hostname);
		auto [old_reader_pool_rc, old_reader_pool] = bgd_connection_pool_count(
			admin, repeat_hgs.green_reader, deployment_a.green_readers[0].hostname);
		diag("deployment B pool mismatch rc=%d blue_echo=%s green_writer_echo=%s "
			"green_reader_echo=%s counts=%lld/%lld/%lld old_pool_rc=%d/%d old_counts=%lld/%lld",
			b_pools.rc, b_pools.blue_echo.c_str(), b_pools.green_writer_echo.c_str(),
			b_pools.green_reader_echo.c_str(), static_cast<long long>(b_pools.blue_count),
			static_cast<long long>(b_pools.green_writer_count),
			static_cast<long long>(b_pools.green_reader_count), old_writer_pool_rc, old_reader_pool_rc,
			static_cast<long long>(old_writer_pool), static_cast<long long>(old_reader_pool));
	}
	ok(green_pools_match(b_pools, deployment_b) &&
		green_pools_are_zero(admin, repeat_hgs, deployment_a),
		"second lifecycle creates only deployment B green destination pools");

	auto [b_initiated_seq_rc, b_initiated_seq] = sim.probe_log_last_sequence();
	rc = b_initiated_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_b), topology_with_reader_pair(deployment_b, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, b_initiated_seq,
		repeat_scenario, repeat_hgs, "deployment B initiated", "WRITER_SWITCHOVER_INITIATED") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS &&
		writer_placement(admin, repeat_hgs, deployment_b, false),
		"deployment B second lifecycle records initiated without stale demotion");

	auto [b_progress_seq_rc, b_progress_seq] = sim.probe_log_last_sequence();
	rc = b_progress_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_b), topology_with_reader_pair(deployment_b, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, b_progress_seq,
		repeat_scenario, repeat_hgs, "deployment B in progress", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	effects_rc = phase_rc == EXIT_SUCCESS ? wait_for_writer_placement(admin, sim, b_progress_seq,
		repeat_scenario, repeat_hgs, deployment_b, "deployment B in-progress effects", true) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"deployment B second lifecycle applies a fresh in-progress writer demotion");

	auto [b_post_seq_rc, b_post_seq] = sim.probe_log_last_sequence();
	rc = b_post_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_b),
		topology_with_reader_pair(deployment_b, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	effects_rc = rc == EXIT_SUCCESS ? wait_for_post_effects(admin, sim, b_post_seq,
		repeat_scenario, repeat_hgs, deployment_b, "deployment B post-processing") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"deployment B post-processing applies fresh blue-writer placement effects");

	pool_rc = effects_rc == EXIT_SUCCESS ? wait_for_pool_drain(admin, sim, b_post_seq,
		repeat_scenario, "deployment B post-processing", repeat_hgs,
		repeat_hgs.blue_writer, deployment_b.blue_writer.hostname) : EXIT_FAILURE;
	ok(pool_rc == EXIT_SUCCESS,
		"deployment B post-processing drains the second lifecycle blue-writer pool");

	auto [b_route_rc, b_route] = pool_rc == EXIT_SUCCESS ?
		connect_and_echo(cl) : rc_t<string> { EXIT_FAILURE, {} };
	ok(b_route_rc == EXIT_SUCCESS && b_route.find(deployment_b.green_writer.ip) != string::npos &&
		b_route.find(deployment_a.green_writer.ip) == string::npos,
		"second lifecycle maps new blue connections only to deployment B green");

	auto [b_completed_seq_rc, b_completed_seq] = sim.probe_log_last_sequence();
	rc = b_completed_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(deployment_b), target_only_completed(deployment_b)) : EXIT_FAILURE;
	phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, b_completed_seq,
		repeat_scenario, repeat_hgs, "deployment B completed", "READER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS,
		"deployment B second lifecycle reaches inferred reader switchover");

	auto [b_empty_seq_rc, b_empty_seq] = sim.probe_log_last_sequence();
	rc = b_empty_seq_rc == EXIT_SUCCESS ? sim.topology_delete(topology_backends(deployment_b)) : EXIT_FAILURE;
	effects_rc = rc == EXIT_SUCCESS ? wait_for_reset_effects(admin, sim, b_empty_seq,
		repeat_scenario, repeat_hgs, deployment_b) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"deployment B present-empty topology independently resets the second FSM lifecycle");
	ok(green_membership_is(admin, repeat_hgs, deployment_b, deployment_a, 1),
		"deployment B reset retains only its TLS-distinct configured green rows");
	ok(green_pools_are_zero(admin, repeat_hgs, deployment_b) &&
		green_pools_are_zero(admin, repeat_hgs, deployment_a),
		"deployment B reset drains B pools without recreating deployment A pool state");

	const string concurrent_scenario = "concurrent-clusters";
	RDS_BGD_Cluster cluster_1 = bgd_cluster_init();
	RDS_BGD_Cluster cluster_1_b = bgd_cluster_1_deployment_b_init();
	RDS_BGD_Cluster cluster_2 = bgd_cluster_2_init();
	RDS_BGD_Cluster cluster_3 = bgd_cluster_3_init();
	BGD_Hostgroups cluster_1_hgs { 1410, 1411, 1412, 1413 };
	BGD_Hostgroups cluster_2_hgs { 1420, 1421, 1422, 1423 };
	BGD_Hostgroups cluster_3_hgs { 1430, 1431, 1432, 1433 };
	vector<Endpoint> concurrent_backends {
		cluster_1.blue_writer.endpoint(), cluster_1.green_writer.endpoint(),
		cluster_1_b.green_writer.endpoint(),
		cluster_2.blue_writer.endpoint(), cluster_2.green_writer.endpoint(),
		cluster_3.blue_writer.endpoint(), cluster_3.green_writer.endpoint(),
	};
	if (reset_scenario(admin, sim, concurrent_backends) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset concurrent-cluster scenario");
	}
	set_role_states(sim, cluster_1);
	set_role_states(sim, cluster_1_b);
	set_role_states(sim, cluster_2);
	set_role_states(sim, cluster_3);

	auto [concurrent_seq_rc, concurrent_seq] = sim.probe_log_last_sequence();
	rc = concurrent_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(cluster_1), topology_with_reader_pair(cluster_1, "AVAILABLE")) : EXIT_FAILURE;
	if (rc == EXIT_SUCCESS) rc = sim.topology_update(
		topology_backends(cluster_2), topology_with_reader_pair(cluster_2, "AVAILABLE"));
	if (rc == EXIT_SUCCESS) rc = sim.topology_update(
		topology_backends(cluster_3), topology_with_reader_pair(cluster_3, "AVAILABLE"));
	int setup_1_rc = rc == EXIT_SUCCESS ? bgd_admin_setup(admin, cluster_1, cluster_1_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ cluster_1.blue_writer, cluster_1.blue_readers[0], cluster_1.blue_readers[1] },
		{ cluster_1.green_writer, cluster_1.green_readers[0] }, 0, 0) : EXIT_FAILURE;
	int setup_2_rc = setup_1_rc == EXIT_SUCCESS ? bgd_admin_setup(admin, cluster_2, cluster_2_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ cluster_2.blue_writer, cluster_2.blue_readers[0], cluster_2.blue_readers[1] },
		{ cluster_2.green_writer, cluster_2.green_readers[0] }, 0, 1) : EXIT_FAILURE;
	int setup_3_rc = setup_2_rc == EXIT_SUCCESS ? bgd_admin_setup(admin, cluster_3, cluster_3_hgs,
		BGD_Admin_Mode::explicit_configuration,
		{ cluster_3.blue_writer, cluster_3.blue_readers[0], cluster_3.blue_readers[1] },
		{ cluster_3.green_writer, cluster_3.green_readers[0] }, 0, 0) : EXIT_FAILURE;

	auto [cluster_1_probe_rc, cluster_1_probe] = setup_3_rc == EXIT_SUCCESS ?
		bgd_wait_for_probe(sim, concurrent_seq, cluster_1.green_writer.endpoint(),
			RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin, concurrent_scenario,
			"cluster 1 available", cluster_1_hgs.blue_writer, all_hostgroups(cluster_1_hgs)) :
		rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	int cluster_1_status_rc = cluster_1_probe_rc == EXIT_SUCCESS ? wait_for_status(
		admin, sim, concurrent_seq, concurrent_scenario, cluster_1_hgs,
		"cluster 1 available", "AVAILABLE") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && setup_1_rc == EXIT_SUCCESS && cluster_1_probe_rc == EXIT_SUCCESS &&
		cluster_1_status_rc == EXIT_SUCCESS,
		"concurrent cluster 1 records AVAILABLE from its own green backend");

	auto [cluster_2_probe_rc, cluster_2_probe] = setup_3_rc == EXIT_SUCCESS ?
		bgd_wait_for_probe(sim, concurrent_seq, cluster_2.green_writer.endpoint(),
			RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 1, admin, concurrent_scenario,
			"cluster 2 available", cluster_2_hgs.blue_writer, all_hostgroups(cluster_2_hgs)) :
		rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	int cluster_2_status_rc = cluster_2_probe_rc == EXIT_SUCCESS ? wait_for_status(
		admin, sim, concurrent_seq, concurrent_scenario, cluster_2_hgs,
		"cluster 2 available", "AVAILABLE") : EXIT_FAILURE;
	ok(setup_2_rc == EXIT_SUCCESS && cluster_2_probe_rc == EXIT_SUCCESS &&
		cluster_2_probe.encrypted && cluster_2_status_rc == EXIT_SUCCESS,
		"concurrent cluster 2 records AVAILABLE from its TLS-distinct green backend");

	auto [cluster_3_probe_rc, cluster_3_probe] = setup_3_rc == EXIT_SUCCESS ?
		bgd_wait_for_probe(sim, concurrent_seq, cluster_3.green_writer.endpoint(),
			RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin, concurrent_scenario,
			"cluster 3 available", cluster_3_hgs.blue_writer, all_hostgroups(cluster_3_hgs)) :
		rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	int cluster_3_status_rc = cluster_3_probe_rc == EXIT_SUCCESS ? wait_for_status(
		admin, sim, concurrent_seq, concurrent_scenario, cluster_3_hgs,
		"cluster 3 available", "AVAILABLE") : EXIT_FAILURE;
	ok(setup_3_rc == EXIT_SUCCESS && cluster_3_probe_rc == EXIT_SUCCESS &&
		cluster_3_status_rc == EXIT_SUCCESS,
		"concurrent cluster 3 records AVAILABLE from its own green backend");

	auto [cluster_1_progress_seq_rc, cluster_1_progress_seq] = sim.probe_log_last_sequence();
	rc = cluster_1_progress_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(cluster_1), topology_with_reader_pair(cluster_1, "SWITCHOVER_IN_PROGRESS")) : EXIT_FAILURE;
	phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, cluster_1_progress_seq,
		concurrent_scenario, cluster_1_hgs, "cluster 1 in progress", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	effects_rc = phase_rc == EXIT_SUCCESS ? wait_for_writer_placement(admin, sim, cluster_1_progress_seq,
		concurrent_scenario, cluster_1_hgs, cluster_1, "cluster 1 in-progress effects", true) : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"advancing cluster 1 alone applies only its in-progress writer demotion");
	ok(status_is(admin, cluster_2_hgs, "AVAILABLE") &&
		writer_placement(admin, cluster_2_hgs, cluster_2, false) &&
		status_is(admin, cluster_3_hgs, "AVAILABLE") &&
		writer_placement(admin, cluster_3_hgs, cluster_3, false),
		"cluster 1 advancement leaves clusters 2 and 3 in AVAILABLE placement");

	auto [cluster_2_post_seq_rc, cluster_2_post_seq] = sim.probe_log_last_sequence();
	rc = cluster_2_post_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(cluster_2),
		topology_with_reader_pair(cluster_2, "SWITCHOVER_IN_POST_PROCESSING")) : EXIT_FAILURE;
	effects_rc = rc == EXIT_SUCCESS ? wait_for_post_effects(admin, sim, cluster_2_post_seq,
		concurrent_scenario, cluster_2_hgs, cluster_2, "cluster 2 post-processing") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"advancing cluster 2 alone applies only its post-processing writer policy");
	ok(status_is(admin, cluster_1_hgs, "WRITER_SWITCHOVER_IN_PROGRESS") &&
		writer_placement(admin, cluster_1_hgs, cluster_1, true) &&
		status_is(admin, cluster_3_hgs, "AVAILABLE") &&
		writer_placement(admin, cluster_3_hgs, cluster_3, false),
		"cluster 2 advancement preserves cluster 1 progress and cluster 3 availability");

	auto [cluster_3_initiated_seq_rc, cluster_3_initiated_seq] = sim.probe_log_last_sequence();
	rc = cluster_3_initiated_seq_rc == EXIT_SUCCESS ? sim.topology_update(
		topology_backends(cluster_3), topology_with_reader_pair(cluster_3, "SWITCHOVER_INITIATED")) : EXIT_FAILURE;
	phase_rc = rc == EXIT_SUCCESS ? wait_for_status(admin, sim, cluster_3_initiated_seq,
		concurrent_scenario, cluster_3_hgs, "cluster 3 initiated", "WRITER_SWITCHOVER_INITIATED") : EXIT_FAILURE;
	ok(rc == EXIT_SUCCESS && phase_rc == EXIT_SUCCESS &&
		writer_placement(admin, cluster_3_hgs, cluster_3, false),
		"advancing cluster 3 alone records initiated without writer demotion");
	ok(status_is(admin, cluster_1_hgs, "WRITER_SWITCHOVER_IN_PROGRESS") &&
		writer_placement(admin, cluster_1_hgs, cluster_1, true) &&
		post_effects(admin, cluster_2_hgs, cluster_2),
		"cluster 3 advancement preserves cluster 1 progress and cluster 2 post-processing");

	auto [cluster_1_replace_seq_rc, cluster_1_replace_seq] = sim.probe_log_last_sequence();
	rc = cluster_1_replace_seq_rc == EXIT_SUCCESS ? replace_green_membership(
		admin, cluster_1_hgs, cluster_1, cluster_1_b, 1) : EXIT_FAILURE;
	if (rc == EXIT_SUCCESS) {
		rc = sim.topology_update(topology_backends(cluster_1_b),
			topology_with_reader_pair(cluster_1_b, "SWITCHOVER_IN_PROGRESS"));
	}
	auto [cluster_1_b_probe_rc, cluster_1_b_probe] = rc == EXIT_SUCCESS ?
		bgd_wait_for_probe(sim, cluster_1_replace_seq, cluster_1_b.green_writer.endpoint(),
			RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 1, admin, concurrent_scenario,
			"cluster 1 replacement", cluster_1_hgs.blue_writer, all_hostgroups(cluster_1_hgs)) :
		rc_t<RDS_BGD_Probe_Log> { EXIT_FAILURE, {} };
	ok(rc == EXIT_SUCCESS && cluster_1_b_probe_rc == EXIT_SUCCESS &&
		cluster_1_b_probe.encrypted &&
		green_membership_is(admin, cluster_1_hgs, cluster_1_b, cluster_1, 1),
		"cluster 1 replacement uses only its new TLS topology and runtime rows");

	phase_rc = cluster_1_b_probe_rc == EXIT_SUCCESS ? wait_for_status(admin, sim,
		cluster_1_replace_seq, concurrent_scenario, cluster_1_hgs,
		"cluster 1 replacement in progress", "WRITER_SWITCHOVER_IN_PROGRESS") : EXIT_FAILURE;
	effects_rc = phase_rc == EXIT_SUCCESS ? wait_for_writer_placement(admin, sim,
		cluster_1_replace_seq, concurrent_scenario, cluster_1_hgs, cluster_1_b,
		"cluster 1 replacement effects", true) : EXIT_FAILURE;
	ok(phase_rc == EXIT_SUCCESS && effects_rc == EXIT_SUCCESS,
		"cluster 1 replacement independently reapplies its existing in-progress phase");

	auto [cluster_1_stale_rc, cluster_1_stale_probe] = bgd_wait_for_probe(sim,
		cluster_1_b_probe.sequence_id, cluster_1.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kNegativeProbeTimeoutMs, -1, admin,
		concurrent_scenario, "cluster 1 stale topology negative probe",
		cluster_1_hgs.blue_writer, all_hostgroups(cluster_1_hgs));
	ok(cluster_1_b_probe_rc == EXIT_SUCCESS && cluster_1_stale_rc == ETIMEDOUT,
		"cluster 1 replacement stops probing its stale deployment topology");

	auto [cluster_2_reprobe_rc, cluster_2_reprobe] = bgd_wait_for_probe(sim,
		cluster_1_replace_seq, cluster_2.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 1, admin,
		concurrent_scenario, "cluster 2 post-replacement probe",
		cluster_2_hgs.blue_writer, all_hostgroups(cluster_2_hgs));
	ok(cluster_2_reprobe_rc == EXIT_SUCCESS && cluster_2_reprobe.encrypted &&
		post_effects(admin, cluster_2_hgs, cluster_2),
		"cluster 1 replacement reprobes cluster 2 and preserves its post-processing phase");

	auto [cluster_3_reprobe_rc, cluster_3_reprobe] = bgd_wait_for_probe(sim,
		cluster_1_replace_seq, cluster_3.green_writer.endpoint(),
		RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, 0, admin,
		concurrent_scenario, "cluster 3 post-replacement probe",
		cluster_3_hgs.blue_writer, all_hostgroups(cluster_3_hgs));
	ok(cluster_3_reprobe_rc == EXIT_SUCCESS && !cluster_3_reprobe.encrypted &&
		status_is(admin, cluster_3_hgs, "WRITER_SWITCHOVER_INITIATED") &&
		writer_placement(admin, cluster_3_hgs, cluster_3, false),
		"cluster 1 replacement reprobes cluster 3 and preserves its initiated phase");

	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS ||
		sim.topology_drop(concurrent_backends) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to clean repeated/concurrent scenarios");
	}
	mysql_close(admin);
	return exit_status();
}
