/**
 * @file test_rds_bgd_probe_tls-t.cpp
 * @brief AWS RDS Blue/Green direct-probe tuple and TLS coverage.
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

vector<Endpoint> cluster_backends(RDS_BGD_Cluster& cluster) {
	vector<Endpoint> backends { cluster.blue_writer.endpoint(), cluster.green_writer.endpoint() };
	for (RDS_BGD_Host& reader : cluster.blue_readers) backends.push_back(reader.endpoint());
	for (RDS_BGD_Host& reader : cluster.green_readers) backends.push_back(reader.endpoint());
	return backends;
}

void set_writers_writable(RDS_BGD_Simulator& sim, RDS_BGD_Cluster& cluster) {
	for (Endpoint& writer : cluster.get_writer_hosts()) {
		if (sim.read_only_update(writer, false) != EXIT_SUCCESS) {
			BAIL_OUT("failed to configure simulated writer read_only state");
		}
	}
}

int scenario_cleanup(MYSQL* admin, RDS_BGD_Simulator& sim, const vector<Endpoint>& backends) {
	if (bgd_admin_cleanup(admin) != EXIT_SUCCESS) return EXIT_FAILURE;
	if (execute_all(admin, {
		"DELETE FROM mysql_hostgroup_attributes",
		"LOAD MYSQL SERVERS TO RUNTIME",
	}) != EXIT_SUCCESS) return EXIT_FAILURE;
	return sim.topology_drop(backends);
}

bool server_row_matches(MYSQL* admin, const string& table, int hostgroup, const RDS_BGD_Host& host, int use_ssl) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT hostgroup_id,hostname,port,use_ssl FROM " + table + " WHERE hostgroup_id=" +
		to_string(hostgroup) + " AND hostname=" + bgd_sql_quote(host.hostname) + " AND port=" +
		to_string(host.port));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 4 &&
		rows[0][0] == to_string(hostgroup) && rows[0][1] == host.hostname &&
		rows[0][2] == "3306" && rows[0][3] == to_string(use_ssl);
}

bool persistent_server_precedes(MYSQL* admin, const RDS_BGD_Host& first, const RDS_BGD_Host& second) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT hostname FROM mysql_servers WHERE hostname IN (" + bgd_sql_quote(first.hostname) + "," +
		bgd_sql_quote(second.hostname) + ") ORDER BY rowid");
	return rc == EXIT_SUCCESS && rows.size() == 2 && rows[0].size() == 1 && rows[1].size() == 1 &&
		rows[0][0] == first.hostname && rows[1][0] == second.hostname;
}

bool blue_backend_and_pool_match(const CommandLine& cl, MYSQL* admin, RDS_BGD_Cluster& cluster,
	const BGD_Hostgroups& hgs)
{
	MYSQL* client = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	auto [echo_rc, echo] = client ? bgd_backend_ip_echo(client) : rc_t<string> { EXIT_FAILURE, {} };
	if (client) mysql_close(client);
	auto [pool_rc, pool] = bgd_connection_pool_count(admin, hgs.blue_writer, cluster.blue_writer.hostname);
	return echo_rc == EXIT_SUCCESS && echo.find(cluster.blue_writer.ip) != string::npos &&
		pool_rc == EXIT_SUCCESS && pool >= 1;
}

int wait_for_available(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence, const string& scenario,
	const BGD_Hostgroups& hgs)
{
	return bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(hgs.blue_writer) + " AND status='AVAILABLE'",
		kTimeoutSeconds, sim, sequence, scenario, "available", "one AVAILABLE runtime BGD row",
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
}

struct Probe_Chain {
	int table_rc;
	RDS_BGD_Probe_Log table;
	int blue_metadata_rc;
	RDS_BGD_Probe_Log blue_metadata;
	int green_metadata_rc;
	RDS_BGD_Probe_Log green_metadata;
};

Probe_Chain wait_for_direct_probe_chain(MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence,
	RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hgs, int blue_use_ssl, int green_use_ssl,
	const string& scenario)
{
	auto [table_rc, table] = bgd_wait_for_probe(sim, sequence, cluster.blue_writer.endpoint(),
		RDS_BGD_Probe_Kind::table_check, kProbeTimeoutMs, blue_use_ssl, admin, scenario, "table check",
		hgs.blue_writer, { hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	const uint64_t metadata_baseline = table_rc == EXIT_SUCCESS ? table.sequence_id : sequence;
	auto [blue_metadata_rc, blue_metadata] = bgd_wait_for_probe(sim, metadata_baseline,
		cluster.blue_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, blue_use_ssl,
		admin, scenario, "blue metadata", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	const uint64_t green_baseline = blue_metadata_rc == EXIT_SUCCESS ? blue_metadata.sequence_id : metadata_baseline;
	auto [green_metadata_rc, green_metadata] = bgd_wait_for_probe(sim, green_baseline,
		cluster.green_writer.endpoint(), RDS_BGD_Probe_Kind::metadata, kProbeTimeoutMs, green_use_ssl,
		admin, scenario, "direct green metadata", hgs.blue_writer,
		{ hgs.blue_writer, hgs.blue_reader, hgs.green_writer, hgs.green_reader });
	return { table_rc, table, blue_metadata_rc, blue_metadata, green_metadata_rc, green_metadata };
}

bool probe_chain_is_ordered(const Probe_Chain& chain, int blue_use_ssl, int green_use_ssl) {
	return chain.table_rc == EXIT_SUCCESS && chain.blue_metadata_rc == EXIT_SUCCESS &&
		chain.green_metadata_rc == EXIT_SUCCESS &&
		chain.table.sequence_id < chain.blue_metadata.sequence_id &&
		chain.blue_metadata.sequence_id < chain.green_metadata.sequence_id &&
		chain.table.probe_kind == RDS_BGD_Probe_Kind::table_check &&
		chain.blue_metadata.probe_kind == RDS_BGD_Probe_Kind::metadata &&
		chain.green_metadata.probe_kind == RDS_BGD_Probe_Kind::metadata &&
		chain.table.encrypted == (blue_use_ssl != 0) &&
		chain.blue_metadata.encrypted == (blue_use_ssl != 0) &&
		chain.green_metadata.encrypted == (green_use_ssl != 0);
}

bool hostgroup_has_no_server(MYSQL* admin, int hostgroup) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT COUNT(*) FROM mysql_servers WHERE hostgroup_id=" + to_string(hostgroup));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "0";
}

bool green_default_is_tls(MYSQL* admin, int hostgroup) {
	auto [rc, rows] = mysql_query_ext_rows(admin,
		"SELECT json_extract(servers_defaults,'$.use_ssl') FROM mysql_hostgroup_attributes WHERE hostgroup_id=" +
		to_string(hostgroup));
	return rc == EXIT_SUCCESS && rows.size() == 1 && rows[0].size() == 1 && rows[0][0] == "1";
}

}  // namespace

int main() {
	plan(15);

	CommandLine cl {};
	if (cl.getEnv()) BAIL_OUT("failed to load TAP environment");
	MYSQL* admin = init_mysql_conn(cl.admin_host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (admin == nullptr) BAIL_OUT("failed to connect to ProxySQL Admin");

	RDS_BGD_Simulator sim {};
	if (sim.connect(cl.host, 3306, cl.username, cl.password) != EXIT_SUCCESS) {
		mysql_close(admin);
		BAIL_OUT("failed to connect to the SQLite3-server simulator");
	}

	// Automatic mode: insert a reader before the writer, with a distinguishable TLS value.
	RDS_BGD_Cluster automatic = bgd_cluster_init();
	BGD_Hostgroups automatic_hgs { 940, 941, 942, 943 };
	if (scenario_cleanup(admin, sim, cluster_backends(automatic)) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset automatic direct-probe scenario");
	}
	set_writers_writable(sim, automatic);
	if (bgd_admin_setup(admin, automatic, automatic_hgs, BGD_Admin_Mode::automatic, {}, {}, 0) != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, automatic, automatic_hgs, { automatic.blue_readers[0] }, false, 0) != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, automatic, automatic_hgs, { automatic.blue_writer }, false, 1) != EXIT_SUCCESS ||
		execute_all(admin, { "LOAD MYSQL SERVERS TO RUNTIME" }) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure automatic direct-probe scenario");
	}
	ok(persistent_server_precedes(admin, automatic.blue_readers[0], automatic.blue_writer) &&
		server_row_matches(admin, "runtime_mysql_servers", automatic_hgs.blue_reader, automatic.blue_readers[0], 0) &&
		server_row_matches(admin, "runtime_mysql_servers", automatic_hgs.blue_writer, automatic.blue_writer, 1),
		"automatic: blue reader is inserted before the writer in monitor input with distinguishable TLS");
	auto [automatic_seq_rc, automatic_seq] = sim.probe_log_last_sequence();
	if (automatic_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read automatic direct-probe baseline");
	int rc = sim.topology_update(automatic.get_writers(), automatic.get_topology("AVAILABLE"));
	int automatic_available_rc = rc == EXIT_SUCCESS ? wait_for_available(admin, sim, automatic_seq,
		"automatic-reader-before-writer", automatic_hgs) : EXIT_FAILURE;
	auto automatic_chain = wait_for_direct_probe_chain(admin, sim, automatic_seq, automatic, automatic_hgs, 1, 1,
		"automatic-reader-before-writer");
	ok(rc == EXIT_SUCCESS && automatic_available_rc == EXIT_SUCCESS,
		"automatic: recorded AVAILABLE observation reaches the runtime BGD worker state");
	ok(probe_chain_is_ordered(automatic_chain, 1, 1),
		"automatic: table-check and metadata probes retain the matched writer backend and TLS in order");
	ok(automatic_chain.green_metadata_rc == EXIT_SUCCESS &&
		automatic_chain.green_metadata.backend.host == automatic.green_writer.ip &&
		automatic_chain.green_metadata.backend.port == 3306 && automatic_chain.green_metadata.encrypted,
		"automatic: direct metadata probe targets the mapped green writer IP and inherits matched blue-writer TLS");
	ok(blue_backend_and_pool_match(cl, admin, automatic, automatic_hgs),
		"automatic: client backend and connection pool remain on the configured blue writer");

	// Explicit mode: a valid-looking distractor is present, but only the exact TARGET may supply TLS.
	RDS_BGD_Cluster explicit_cluster = bgd_cluster_2_init();
	RDS_BGD_Cluster distractor_cluster = bgd_cluster_1_deployment_b_init();
	BGD_Hostgroups explicit_hgs { 950, 951, 952, 953 };
	vector<Endpoint> explicit_backends = cluster_backends(explicit_cluster);
	explicit_backends.push_back(distractor_cluster.green_writer.endpoint());
	if (scenario_cleanup(admin, sim, explicit_backends) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset explicit direct-probe scenario");
	}
	set_writers_writable(sim, explicit_cluster);
	if (bgd_admin_setup(admin, explicit_cluster, explicit_hgs, BGD_Admin_Mode::explicit_configuration,
		{ explicit_cluster.blue_writer }, {}, 0) != EXIT_SUCCESS ||
		execute_all(admin, {
			"INSERT INTO mysql_servers(hostgroup_id,hostname,port,status,use_ssl,comment) VALUES (" +
			to_string(explicit_hgs.green_writer) + "," + bgd_sql_quote(distractor_cluster.green_writer.hostname) +
			",3306,'ONLINE',0,'BGD TAP TLS distractor')",
			"INSERT INTO mysql_servers(hostgroup_id,hostname,port,status,use_ssl,comment) VALUES (" +
			to_string(explicit_hgs.green_writer) + "," + bgd_sql_quote(explicit_cluster.green_writer.hostname) +
			",3306,'ONLINE',1,'BGD TAP exact TARGET')",
			"LOAD MYSQL SERVERS TO RUNTIME",
		}) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure explicit direct-probe scenario");
	}
	ok(persistent_server_precedes(admin, distractor_cluster.green_writer, explicit_cluster.green_writer) &&
		server_row_matches(admin, "mysql_servers", explicit_hgs.green_writer, explicit_cluster.green_writer, 1) &&
		server_row_matches(admin, "runtime_mysql_servers", explicit_hgs.green_writer, explicit_cluster.green_writer, 1) &&
		server_row_matches(admin, "runtime_mysql_servers", explicit_hgs.green_writer, distractor_cluster.green_writer, 0),
		"explicit: first valid-looking distractor and exact TARGET rows have distinct runtime/Admin TLS values");
	auto [explicit_seq_rc, explicit_seq] = sim.probe_log_last_sequence();
	if (explicit_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read explicit direct-probe baseline");
	rc = sim.topology_update(explicit_cluster.get_writers(), explicit_cluster.get_topology("AVAILABLE"));
	int explicit_available_rc = rc == EXIT_SUCCESS ? wait_for_available(admin, sim, explicit_seq,
		"explicit-target-identity", explicit_hgs) : EXIT_FAILURE;
	auto explicit_chain = wait_for_direct_probe_chain(admin, sim, explicit_seq, explicit_cluster, explicit_hgs, 0, 1,
		"explicit-target-identity");
	ok(rc == EXIT_SUCCESS && explicit_available_rc == EXIT_SUCCESS,
		"explicit: recorded AVAILABLE observation reaches the configured runtime BGD worker state");
	ok(probe_chain_is_ordered(explicit_chain, 0, 1),
		"explicit: table-check and metadata probes preserve blue plaintext and exact TARGET TLS in order");
	ok(explicit_chain.green_metadata_rc == EXIT_SUCCESS &&
		explicit_chain.green_metadata.backend.host == explicit_cluster.green_writer.ip &&
		explicit_chain.green_metadata.backend.host != distractor_cluster.green_writer.ip &&
		explicit_chain.green_metadata.backend.port == 3306 && explicit_chain.green_metadata.encrypted,
		"explicit: direct metadata probe selects the exact TARGET hostname rather than the TLS distractor");
	ok(blue_backend_and_pool_match(cl, admin, explicit_cluster, explicit_hgs),
		"explicit: client backend and connection pool remain on the configured blue writer");

	// An empty explicit green writer hostgroup gets its TLS from servers_defaults when BGD creates TARGET.
	RDS_BGD_Cluster defaults_cluster = bgd_cluster_3_init();
	BGD_Hostgroups defaults_hgs { 960, 961, 962, 963 };
	if (scenario_cleanup(admin, sim, cluster_backends(defaults_cluster)) != EXIT_SUCCESS) {
		BAIL_OUT("failed to reset defaults-created direct-probe scenario");
	}
	set_writers_writable(sim, defaults_cluster);
	if (bgd_admin_setup(admin, defaults_cluster, defaults_hgs, BGD_Admin_Mode::explicit_configuration,
		{ defaults_cluster.blue_writer }, {}, 0) != EXIT_SUCCESS ||
		execute_all(admin, {
			"INSERT INTO mysql_hostgroup_attributes(hostgroup_id,servers_defaults) VALUES (" +
			to_string(defaults_hgs.green_writer) + ",' {\"use_ssl\":1 }')",
			"LOAD MYSQL SERVERS TO RUNTIME",
		}) != EXIT_SUCCESS) {
		BAIL_OUT("failed to configure defaults-created direct-probe scenario");
	}
	ok(hostgroup_has_no_server(admin, defaults_hgs.green_writer) && green_default_is_tls(admin, defaults_hgs.green_writer),
		"defaults-created: explicit green writer hostgroup starts empty with persistent TLS defaults");
	auto [defaults_seq_rc, defaults_seq] = sim.probe_log_last_sequence();
	if (defaults_seq_rc != EXIT_SUCCESS) BAIL_OUT("failed to read defaults-created direct-probe baseline");
	rc = sim.topology_update(defaults_cluster.get_writers(), defaults_cluster.get_topology("AVAILABLE"));
	int defaults_available_rc = rc == EXIT_SUCCESS ? wait_for_available(admin, sim, defaults_seq,
		"defaults-created-target", defaults_hgs) : EXIT_FAILURE;
	int defaults_server_rc = defaults_available_rc == EXIT_SUCCESS ? bgd_wait_for_condition(admin,
		"SELECT COUNT(*)=1 FROM runtime_mysql_servers WHERE hostgroup_id=" + to_string(defaults_hgs.green_writer) +
		" AND hostname=" + bgd_sql_quote(defaults_cluster.green_writer.hostname) + " AND port=3306 AND use_ssl=1",
		kTimeoutSeconds, sim, defaults_seq, "defaults-created-target", "green creation",
		"one TLS TARGET runtime server", defaults_hgs.blue_writer,
		{ defaults_hgs.blue_writer, defaults_hgs.blue_reader, defaults_hgs.green_writer, defaults_hgs.green_reader }) : EXIT_FAILURE;
	auto defaults_chain = wait_for_direct_probe_chain(admin, sim, defaults_seq, defaults_cluster, defaults_hgs, 0, 1,
		"defaults-created-target");
	ok(rc == EXIT_SUCCESS && defaults_available_rc == EXIT_SUCCESS && defaults_server_rc == EXIT_SUCCESS &&
		server_row_matches(admin, "runtime_mysql_servers", defaults_hgs.green_writer, defaults_cluster.green_writer, 1),
		"defaults-created: AVAILABLE discovery creates the exact TARGET runtime row with TLS defaults applied");
	ok(probe_chain_is_ordered(defaults_chain, 0, 1),
		"defaults-created: table-check and metadata probes retain the selected backend and TLS in order");
	ok(defaults_chain.green_metadata_rc == EXIT_SUCCESS &&
		defaults_chain.green_metadata.backend.host == defaults_cluster.green_writer.ip &&
		defaults_chain.green_metadata.backend.port == 3306 && defaults_chain.green_metadata.encrypted,
		"defaults-created: direct TARGET metadata probe uses TLS after runtime row creation");
	ok(blue_backend_and_pool_match(cl, admin, defaults_cluster, defaults_hgs),
		"defaults-created: client backend and connection pool remain on the configured blue writer");

	if (scenario_cleanup(admin, sim, cluster_backends(defaults_cluster)) != EXIT_SUCCESS) {
		diag("failed to clean final BGD TAP Admin state");
	}
	mysql_close(admin);
	return exit_status();
}
