#ifndef TAP_TESTS_RDS_BGD_TAP_H
#define TAP_TESTS_RDS_BGD_TAP_H

#include <cstdlib>
#include <cstdint>
#include <cerrno>
#include <string>
#include <vector>

#include "rds_bgd_simulator.h"
#include "tap.h"

using namespace std;

inline int execute_all(MYSQL* admin, vector<string> queries);

inline RDS_BGD_Cluster bgd_cluster_init() {
	return {
		{ "db-1.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.11", 3306 },
		{ "db-1-green-iqu47r.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.14", 3306 },
		{
			{ "db-1-reader-1.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.12", 3306 },
			{ "db-1-reader-2.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.13", 3306 },
		},
		{
			{ "db-1-reader-1-green-dlzky7.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.15", 3306 },
			{ "db-1-reader-2-green-3fpjuu.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.16", 3306 },
		},
	};
}

inline RDS_BGD_Cluster bgd_cluster_1_deployment_b_init() {
	return {
		{ "db-1.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.11", 3306 },
		{ "db-1-green-s7m2kx.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.17", 3306 },
		{
			{ "db-1-reader-1.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.12", 3306 },
			{ "db-1-reader-2.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.13", 3306 },
		},
		{
			{ "db-1-reader-1-green-v4n8qp.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.18", 3306 },
			{ "db-1-reader-2-green-w6h3rz.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.19", 3306 },
		},
	};
}

inline RDS_BGD_Cluster bgd_cluster_2_init() {
	return {
		{ "db-2.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.20", 3306 },
		{ "db-2-green-iqu47r.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.23", 3306 },
		{
			{ "db-2-reader-1.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.21", 3306 },
			{ "db-2-reader-2.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.22", 3306 },
		},
		{
			{ "db-2-reader-1-green-dlzky7.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.24", 3306 },
			{ "db-2-reader-2-green-3fpjuu.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.25", 3306 },
		},
	};
}

inline RDS_BGD_Cluster bgd_cluster_3_init() {
	return {
		{ "db-3.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.26", 3306 },
		{ "db-3-green-iqu47r.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.29", 3306 },
		{
			{ "db-3-reader-1.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.27", 3306 },
			{ "db-3-reader-2.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.28", 3306 },
		},
		{
			{ "db-3-reader-1-green-dlzky7.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.30", 3306 },
			{ "db-3-reader-2-green-3fpjuu.c1yqcg0ie39o.eu-north-1.rds.amazonaws.com", "127.10.0.31", 3306 },
		},
	};
}

enum class BGD_Admin_Mode {
	automatic,
	explicit_configuration,
};

struct BGD_Hostgroups {
	int blue_writer;
	int blue_reader;
	int green_writer;
	int green_reader;
};

inline string bgd_sql_quote(const string& value) {
	string quoted { "'" };
	for (char c : value) {
		quoted += c;
		if (c == '\'') quoted += '\'';
	}
	quoted += '\'';
	return quoted;
}

inline int bgd_admin_cleanup(MYSQL* admin) {
	return execute_all(admin, {
		"DELETE FROM mysql_servers",
		"DELETE FROM mysql_replication_hostgroups",
		"DELETE FROM mysql_aws_rds_bgd_hostgroups",
		"UPDATE mysql_users SET default_hostgroup=0 WHERE username='testuser'",
		"LOAD MYSQL SERVERS TO RUNTIME",
		"LOAD MYSQL USERS TO RUNTIME",
	});
}

inline int bgd_admin_add_servers(
	MYSQL* admin, const RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hostgroups,
	const vector<RDS_BGD_Host>& hosts, bool green, int use_ssl)
{
	vector<string> queries {};
	for (const RDS_BGD_Host& host : hosts) {
		int hostgroup = hostgroups.blue_reader;
		if (!green && host.hostname == cluster.blue_writer.hostname) {
			hostgroup = hostgroups.blue_writer;
		} else if (green && host.hostname == cluster.green_writer.hostname) {
			hostgroup = hostgroups.green_writer;
		} else if (green) {
			hostgroup = hostgroups.green_reader;
		}
		queries.push_back(
			"INSERT INTO mysql_servers(hostgroup_id,hostname,port,status,use_ssl,comment) VALUES (" +
			to_string(hostgroup) + "," + bgd_sql_quote(host.hostname) + "," +
			to_string(host.port) + ",'ONLINE'," + to_string(use_ssl) + "," +
			bgd_sql_quote("BGD TAP " + (green ? string("green ") : string("blue ")) + host.ip) + ")");
	}
	return execute_all(admin, queries);
}

inline int bgd_admin_setup(
	MYSQL* admin, const RDS_BGD_Cluster& cluster, const BGD_Hostgroups& hostgroups,
	BGD_Admin_Mode mode, const vector<RDS_BGD_Host>& blue_hosts,
	const vector<RDS_BGD_Host>& green_hosts = {}, int blue_use_ssl = 0, int green_use_ssl = 0)
{
	vector<string> queries {
		"INSERT INTO mysql_replication_hostgroups(writer_hostgroup,reader_hostgroup) VALUES (" +
			to_string(hostgroups.blue_writer) + "," + to_string(hostgroups.blue_reader) + ")",
		"SET mysql-monitor_username='testuser'",
		"SET mysql-monitor_password='testuser'",
		"SET mysql-monitor_enabled='true'",
		"SET mysql-monitor_read_only_interval=100",
		"SET mysql-monitor_aws_rds_topology_discovery_interval=1",
		"SET mysql-aws_blue_green_deployment_auto_discovery='" +
			string(mode == BGD_Admin_Mode::automatic ? "true" : "false") + "'",
		"UPDATE mysql_users SET default_hostgroup=" + to_string(hostgroups.blue_writer) +
			" WHERE username='testuser'",
	};
	if (mode == BGD_Admin_Mode::explicit_configuration) {
		queries.push_back(
			"INSERT INTO mysql_aws_rds_bgd_hostgroups("
			"writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup,"
			"active,writer_is_also_reader,check_interval_ms,check_timeout_ms,comment) VALUES (" +
			to_string(hostgroups.blue_writer) + "," + to_string(hostgroups.blue_reader) + "," +
			to_string(hostgroups.green_writer) + "," + to_string(hostgroups.green_reader) +
			",1,0,100,800,'BGD TAP explicit configuration')");
	}
	if (execute_all(admin, queries) != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, cluster, hostgroups, blue_hosts, false, blue_use_ssl) != EXIT_SUCCESS ||
		bgd_admin_add_servers(admin, cluster, hostgroups, green_hosts, true, green_use_ssl) != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	return execute_all(admin, {
		"LOAD MYSQL VARIABLES TO RUNTIME",
		"LOAD MYSQL USERS TO RUNTIME",
		"LOAD MYSQL SERVERS TO RUNTIME",
	});
}

inline rc_t<vector<mysql_res_row>> bgd_runtime_rows(MYSQL* admin, int writer_hostgroup) {
	return mysql_query_ext_rows(
		admin,
		"SELECT writer_hostgroup,reader_hostgroup,green_writer_hostgroup,green_reader_hostgroup,"
		"auto_generated,status FROM runtime_mysql_aws_rds_bgd_hostgroups WHERE writer_hostgroup=" +
		to_string(writer_hostgroup));
}

inline rc_t<vector<mysql_res_row>> bgd_runtime_servers(MYSQL* admin, const vector<int>& hostgroups) {
	string predicate {};
	for (size_t i = 0; i < hostgroups.size(); ++i) {
		if (i != 0) predicate += ",";
		predicate += to_string(hostgroups[i]);
	}
	return mysql_query_ext_rows(
		admin,
		"SELECT hostgroup_id,hostname,port,status,use_ssl FROM runtime_mysql_servers WHERE hostgroup_id IN (" +
		predicate + ") ORDER BY hostgroup_id,hostname,port");
}

inline rc_t<int64_t> bgd_connection_pool_count(MYSQL* admin, int hostgroup, const string& hostname = "") {
	string query {
		"SELECT COALESCE(SUM(ConnUsed+ConnFree),0) FROM stats_mysql_connection_pool WHERE hostgroup=" +
		to_string(hostgroup)
	};
	if (!hostname.empty()) query += " AND srv_host=" + bgd_sql_quote(hostname);
	auto [rc, rows] = mysql_query_ext_rows(admin, query);
	if (rc != EXIT_SUCCESS || rows.size() != 1 || rows.front().size() != 1) return { EXIT_FAILURE, 0 };
	return { EXIT_SUCCESS, strtoll(rows.front().front().c_str(), nullptr, 10) };
}

inline rc_t<string> bgd_backend_ip_echo(MYSQL* proxy) {
	auto [rc, rows] = mysql_query_ext_rows(proxy, "SELECT @@version_comment LIMIT 1");
	if (rc != EXIT_SUCCESS || rows.size() != 1 || rows.front().size() != 1) return { EXIT_FAILURE, {} };
	return { EXIT_SUCCESS, rows.front().front() };
}

inline void bgd_dump_probe_log_since(RDS_BGD_Simulator& sim, uint64_t sequence) {
	auto [rc, logs] = sim.probe_log_since(sequence);
	if (rc != EXIT_SUCCESS) {
		diag("Unable to read BGD probe diagnostics after sequence %llu", static_cast<unsigned long long>(sequence));
		return;
	}
	for (const RDS_BGD_Probe_Log& log : logs) {
		diag("BGD probe sequence=%llu backend=%s:%d kind=%s encrypted=%d",
			static_cast<unsigned long long>(log.sequence_id), log.backend.host.c_str(), log.backend.port,
			log.probe_kind == RDS_BGD_Probe_Kind::table_check ? "table_check" : "metadata",
			log.encrypted ? 1 : 0);
	}
}

inline void bgd_timeout_diagnostics(
	MYSQL* admin, RDS_BGD_Simulator& sim, uint64_t sequence, const string& scenario,
	const string& phase, const string& expected, int writer_hostgroup, const vector<int>& server_hostgroups)
{
	diag("BGD timeout scenario=%s phase=%s expected=%s", scenario.c_str(), phase.c_str(), expected.c_str());
	auto [bgd_rc, bgd_rows] = bgd_runtime_rows(admin, writer_hostgroup);
	if (bgd_rc == EXIT_SUCCESS) {
		for (const mysql_res_row& row : bgd_rows) {
			string values {};
			for (size_t i = 0; i < row.size(); ++i) {
				if (i != 0) values += ",";
				values += row[i];
			}
			diag("runtime BGD row: %s", values.c_str());
		}
	}
	auto [servers_rc, servers] = bgd_runtime_servers(admin, server_hostgroups);
	if (servers_rc == EXIT_SUCCESS) {
		for (const mysql_res_row& row : servers) {
			if (row.size() == 5) diag("runtime server row: hg=%s host=%s port=%s status=%s ssl=%s",
				row[0].c_str(), row[1].c_str(), row[2].c_str(), row[3].c_str(), row[4].c_str());
		}
	}
	for (int hostgroup : server_hostgroups) {
		auto [pool_rc, count] = bgd_connection_pool_count(admin, hostgroup);
		if (pool_rc == EXIT_SUCCESS) diag("connection pool count hostgroup=%d count=%lld", hostgroup, static_cast<long long>(count));
	}
	bgd_dump_probe_log_since(sim, sequence);
}

inline int bgd_wait_for_condition(
	MYSQL* admin, const string& query, uint32_t timeout_seconds, RDS_BGD_Simulator& sim,
	uint64_t sequence, const string& scenario, const string& phase, const string& expected,
	int writer_hostgroup, const vector<int>& server_hostgroups)
{
	int rc = wait_for_cond(admin, query, timeout_seconds);
	if (rc != EXIT_SUCCESS) {
		bgd_timeout_diagnostics(admin, sim, sequence, scenario, phase, expected, writer_hostgroup, server_hostgroups);
	}
	return rc;
}

inline rc_t<RDS_BGD_Probe_Log> bgd_wait_for_probe(
	RDS_BGD_Simulator& sim, uint64_t sequence, Endpoint backend, RDS_BGD_Probe_Kind kind,
	uint32_t timeout_ms, int encrypted, MYSQL* admin, const string& scenario, const string& phase,
	int writer_hostgroup, const vector<int>& server_hostgroups)
{
	auto [rc, probe] = sim.wait_for_probe_log(sequence, backend, kind, timeout_ms, encrypted);
	if (rc != EXIT_SUCCESS) {
		bgd_timeout_diagnostics(
			admin, sim, sequence, scenario, phase,
			"probe " + backend.host + ":" + to_string(backend.port), writer_hostgroup, server_hostgroups);
	}
	return { rc, probe };
}

inline rc_t<RDS_BGD_Probe_Log> bgd_wait_for_probe_from_backends(
	RDS_BGD_Simulator& sim, uint64_t sequence, const vector<Endpoint>& backends,
	RDS_BGD_Probe_Kind kind, uint32_t timeout_ms, int encrypted = -1)
{
	const uint64_t deadline = monotonic_time() + static_cast<uint64_t>(timeout_ms) * 1000;
	do {
		auto [rc, logs] = sim.probe_log_since(sequence);
		if (rc != EXIT_SUCCESS) return { EXIT_FAILURE, {} };
		for (const RDS_BGD_Probe_Log& log : logs) {
			for (const Endpoint& backend : backends) {
				if (log.backend.host == backend.host && log.backend.port == backend.port && log.probe_kind == kind &&
					(encrypted < 0 || log.encrypted == (encrypted != 0))) return { EXIT_SUCCESS, log };
			}
		}
		usleep(50000);
	} while (monotonic_time() < deadline);
	return { ETIMEDOUT, {} };
}

inline rc_t<RDS_BGD_Probe_Log> bgd_wait_for_any_probe_from_backends(
	RDS_BGD_Simulator& sim, uint64_t sequence, const vector<Endpoint>& backends,
	uint32_t timeout_ms)
{
	const uint64_t deadline = monotonic_time() + static_cast<uint64_t>(timeout_ms) * 1000;
	do {
		auto [rc, logs] = sim.probe_log_since(sequence);
		if (rc != EXIT_SUCCESS) return { EXIT_FAILURE, {} };
		for (const RDS_BGD_Probe_Log& log : logs) {
			for (const Endpoint& backend : backends) {
				if (log.backend.host == backend.host && log.backend.port == backend.port) return { EXIT_SUCCESS, log };
			}
		}
		usleep(50000);
	} while (monotonic_time() < deadline);
	return { ETIMEDOUT, {} };
}

inline int execute_all(MYSQL* admin, vector<string> queries) {
	for (string& query : queries) {
		if (mysql_query(admin, query.c_str()) != 0) {
			diag("Admin query failed (%u): %s; query: %s", mysql_errno(admin), mysql_error(admin), query.c_str());
			return EXIT_FAILURE;
		}
	}
	return EXIT_SUCCESS;
}

#endif  // TAP_TESTS_RDS_BGD_TAP_H
