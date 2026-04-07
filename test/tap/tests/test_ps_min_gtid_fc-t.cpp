/**
 * @file test_ps_min_gtid_fc-t.cpp
 * @brief Test for min_gtid preservation in prepared statements with first_comment_parsing mode 1
 *
 * This test verifies that min_gtid annotations are preserved across STMT_PREPARE and STMT_EXECUTE
 * when using mysql-query_processor_first_comment_parsing=1 (parse before rules).
 *
 * Issue: https://github.com/sysown/proxysql/issues/5384
 *
 * Test flow:
 * 1. Set first_comment_parsing=1 and install a rewrite rule to strip min_gtid
 * 2. Phase A: Prepare/execute with a reachable GTID - should succeed
 * 3. Phase B: Prepare/execute with a future GTID - should fail with timeout
 * 4. Both phases use the same normalized SQL (rewrite rule strips min_gtid)
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <string>
#include <map>
#include "mysql.h"
#include "tap.h"
#include "command_line.h"
#include "utils.h"


/**
 * @brief Hostgroup ID used to isolate test queries to a single known backend.
 */
static const int DEDICATED_HG = 59999;

static int setup(MYSQL* admin, MYSQL* proxy, const CommandLine& cl) {
	diag("========== Setup ==========");

	MYSQL_QUERY_T(proxy, "CREATE DATABASE IF NOT EXISTS test");
	MYSQL_QUERY_T(proxy, "DROP TABLE IF EXISTS test.ps_min_gtid_fc");
	MYSQL_QUERY_T(proxy, "CREATE TABLE test.ps_min_gtid_fc (id INT PRIMARY KEY)");
	MYSQL_QUERY_T(proxy, "INSERT INTO test.ps_min_gtid_fc VALUES (1)");

	// Create a dedicated hostgroup with a single known backend server.
	// This ensures that min_gtid checks are evaluated against the same
	// server whose GTID we fetch from stats, avoiding flakiness when
	// multiple backends are connected in CI.
	char server_query[512];
	snprintf(server_query, sizeof(server_query),
		"INSERT OR REPLACE INTO mysql_servers (hostgroup_id, hostname, port) VALUES (%d, '%s', %d)",
		DEDICATED_HG, cl.mysql_host, cl.mysql_port);
	MYSQL_QUERY_T(admin, server_query);
	MYSQL_QUERY_T(admin, "LOAD MYSQL SERVERS TO RUNTIME");

	MYSQL_QUERY_T(admin, "DELETE FROM mysql_query_rules");
	char rule_query[1024];
	snprintf(rule_query, sizeof(rule_query),
		"INSERT INTO mysql_query_rules (rule_id, active, match_pattern, replace_pattern, apply, destination_hostgroup, comment)"
		" VALUES (42, 1, ';min_gtid=[\\:\\-\\w]+', '', 1, %d, 'Remove min_gtid annotation and route to dedicated HG')",
		DEDICATED_HG);
	MYSQL_QUERY_T(admin, rule_query);
	MYSQL_QUERY_T(admin, "LOAD MYSQL QUERY RULES TO RUNTIME");

	MYSQL_QUERY_T(admin, "SET mysql-query_processor_first_comment_parsing = 1");
	MYSQL_QUERY_T(admin, "LOAD MYSQL VARIABLES TO RUNTIME");
}

static int cleanup(MYSQL* admin) {
	diag("========== Teardown ==========");

	MYSQL_QUERY_T(admin, "DELETE FROM mysql_query_rules WHERE rule_id=42");
	MYSQL_QUERY_T(admin, "LOAD MYSQL QUERY RULES TO RUNTIME");
	MYSQL_QUERY_T(admin, "SET mysql-query_processor_first_comment_parsing = 2");
	MYSQL_QUERY_T(admin, "LOAD MYSQL VARIABLES TO RUNTIME");

	char del_query[256];
	snprintf(del_query, sizeof(del_query),
		"DELETE FROM mysql_servers WHERE hostgroup_id=%d", DEDICATED_HG);
	MYSQL_QUERY_T(admin, del_query);
	MYSQL_QUERY_T(admin, "LOAD MYSQL SERVERS TO RUNTIME");
}

static std::string strip_dashes(const std::string& uuid) {
	std::string result;
	result.reserve(uuid.size());
	for (char c : uuid) {
		if (c != '-') result.push_back(c);
	}
	return result;
}

static bool parse_interval_token(const std::string& token, uint64_t& interval_end) {
	size_t dash_pos = token.find('-');
	if (dash_pos == std::string::npos) {
		interval_end = std::stoull(token);
		return true;
	}
	std::string to_str = token.substr(dash_pos + 1);
	if (to_str.empty()) return false;
	interval_end = std::stoull(to_str);
	return true;
}

static int parse_gtid_executed_max(const std::string& gtid_executed, std::string& server_uuid, uint64_t& max_trxid) {
	if (gtid_executed.empty()) return -1;

	std::map<std::string, uint64_t> uuid_to_max;

	size_t pos = 0;
	while (pos < gtid_executed.size()) {
		size_t comma_pos = gtid_executed.find(',', pos);
		std::string group = (comma_pos == std::string::npos)
			? gtid_executed.substr(pos)
			: gtid_executed.substr(pos, comma_pos - pos);

		size_t colon_pos = group.find(':');
		if (colon_pos == std::string::npos || colon_pos == 0) {
			return -1;
		}

		std::string uuid = strip_dashes(group.substr(0, colon_pos));
		std::string intervals_str = group.substr(colon_pos + 1);

		if (uuid.empty() || intervals_str.empty()) {
			return -1;
		}

		uint64_t group_max = 0;
		bool group_valid = false;

		size_t ipos = 0;
		while (ipos < intervals_str.size()) {
			size_t next_colon = intervals_str.find(':', ipos);
			std::string token = (next_colon == std::string::npos)
				? intervals_str.substr(ipos)
				: intervals_str.substr(ipos, next_colon - ipos);

			if (!token.empty()) {
				uint64_t interval_end = 0;
				if (parse_interval_token(token, interval_end)) {
					if (!group_valid || interval_end > group_max) {
						group_max = interval_end;
					}
					group_valid = true;
				}
			}

			ipos = (next_colon == std::string::npos) ? std::string::npos : next_colon + 1;
		}

		if (group_valid) {
			auto it = uuid_to_max.find(uuid);
			if (it == uuid_to_max.end() || group_max > it->second) {
				uuid_to_max[uuid] = group_max;
			}
		}

		pos = (comma_pos == std::string::npos) ? std::string::npos : comma_pos + 1;
	}

	if (uuid_to_max.empty()) return -1;

	uint64_t best_max = 0;
	std::string best_uuid;
	for (const auto& entry : uuid_to_max) {
		if (entry.second > best_max || (entry.second == best_max && (best_uuid.empty() || entry.first < best_uuid))) {
			best_max = entry.second;
			best_uuid = entry.first;
		}
	}

	server_uuid = best_uuid;
	max_trxid = best_max;
	return 0;
}

static int get_gtid_info(MYSQL* admin, const CommandLine& cl, std::string& server_uuid, uint64_t& max_trxid) {
	char query[512];
	snprintf(query, sizeof(query),
		"SELECT hostname, port, gtid_executed FROM stats.stats_mysql_gtid_executed"
		" WHERE hostname='%s' AND port=%d AND gtid_executed IS NOT NULL AND gtid_executed != ''",
		cl.mysql_host, cl.mysql_port);

	MYSQL_QUERY_T(admin, query);
	MYSQL_RES* res = mysql_store_result(admin);
	if (!res) return -1;

	MYSQL_ROW row = mysql_fetch_row(res);
	if (!row || !row[2]) {
		mysql_free_result(res);
		diag("No GTID info for backend %s:%d", cl.mysql_host, cl.mysql_port);
		return -1;
	}

	std::string gtid_executed = row[2];
	diag("Using GTID from backend %s:%d: %s", cl.mysql_host, cl.mysql_port, gtid_executed.c_str());
	mysql_free_result(res);

	return parse_gtid_executed_max(gtid_executed, server_uuid, max_trxid);
}

static int check_ps_cache(MYSQL* admin, const char* pattern, std::string& query_text, long& global_stmt_id) {
	std::string q = "SELECT global_stmt_id, query FROM stats.stats_mysql_prepared_statements_info WHERE query LIKE '%";
	q += pattern;
	q += "%'";

	MYSQL_QUERY_T(admin, q.c_str());
	MYSQL_RES* res = mysql_store_result(admin);
	if (!res) return -1;

	MYSQL_ROW row = mysql_fetch_row(res);
	if (!row) {
		mysql_free_result(res);
		return -1;
	}

	global_stmt_id = std::stol(row[0]);
	query_text = row[1];
	mysql_free_result(res);
	return 0;
}

static long test_prepare_stmt_valid_gtid(MYSQL* admin, MYSQL* proxy, const std::string& current_gtid) {
	diag("========== Test 1: Valid GTID ==========");

	std::string query_a = "/*+ ;min_gtid=" + current_gtid + " */ SELECT id FROM test.ps_min_gtid_fc WHERE id=?";
	MYSQL_STMT* stmt_a = mysql_stmt_init(proxy);
	if (!stmt_a) {
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_valid_gtid: mysql_stmt_init() failed");
	}

	if (mysql_stmt_prepare(stmt_a, query_a.c_str(), query_a.length()) != 0) {
		std::string stmt_err = mysql_stmt_error(stmt_a) ? mysql_stmt_error(stmt_a) : "(null)";
		mysql_stmt_close(stmt_a);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_valid_gtid: mysql_stmt_prepare() failed: %s", stmt_err.c_str());
	}

	MYSQL_BIND bind_a;
	memset(&bind_a, 0, sizeof(bind_a));
	int id_val = 1;
	unsigned long len = 0;
	my_bool is_null = 0;
	bind_a.buffer_type = MYSQL_TYPE_LONG;
	bind_a.buffer = &id_val;
	bind_a.buffer_length = sizeof(id_val);
	bind_a.length = &len;
	bind_a.is_null = &is_null;

	if (mysql_stmt_bind_param(stmt_a, &bind_a) != 0) {
		std::string stmt_err = mysql_stmt_error(stmt_a) ? mysql_stmt_error(stmt_a) : "(null)";
		mysql_stmt_close(stmt_a);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_valid_gtid: mysql_stmt_bind_param() failed: %s", stmt_err.c_str());
	}

	if (mysql_stmt_execute(stmt_a) != 0) {
		std::string stmt_err = mysql_stmt_error(stmt_a) ? mysql_stmt_error(stmt_a) : "(null)";
		mysql_stmt_close(stmt_a);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_valid_gtid: mysql_stmt_execute() failed: %s", stmt_err.c_str());
	}

	MYSQL_BIND bind_result;
	memset(&bind_result, 0, sizeof(bind_result));
	int result_id = 0;
	bind_result.buffer_type = MYSQL_TYPE_LONG;
	bind_result.buffer = &result_id;
	bind_result.buffer_length = sizeof(result_id);

	if (mysql_stmt_bind_result(stmt_a, &bind_result) != 0) {
		std::string stmt_err = mysql_stmt_error(stmt_a) ? mysql_stmt_error(stmt_a) : "(null)";
		mysql_stmt_close(stmt_a);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_valid_gtid: mysql_stmt_bind_result() failed: %s", stmt_err.c_str());
	}

	if (mysql_stmt_fetch(stmt_a) != 0) {
		std::string stmt_err = mysql_stmt_error(stmt_a) ? mysql_stmt_error(stmt_a) : "(null)";
		mysql_stmt_close(stmt_a);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_valid_gtid: mysql_stmt_fetch() failed: %s", stmt_err.c_str());
	}

	ok(result_id == 1, "test_valid_gtid: Execute with valid GTID succeeded, result=%d", result_id);

	mysql_stmt_free_result(stmt_a);

	std::string query_text_a;
	long global_stmt_id_a = 0;
	if (check_ps_cache(admin, "test.ps_min_gtid_fc", query_text_a, global_stmt_id_a) == 0) {
		ok(query_text_a.find("min_gtid=") == std::string::npos,
		   "test_valid_gtid: Cached query should not contain min_gtid annotation (rewritten): %s", query_text_a.c_str());
	} else {
		mysql_stmt_close(stmt_a);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_valid_gtid: Failed to check PS cache");
	}

	mysql_stmt_close(stmt_a);
	return global_stmt_id_a;
}

static void test_prepare_stmt_future_gtid(MYSQL* admin, MYSQL* proxy, const std::string& future_gtid, long global_stmt_id_a) {
	diag("========== Test 2: Future GTID ==========");

	std::string query_b = "/*+ ;min_gtid=" + future_gtid + " */ SELECT id FROM test.ps_min_gtid_fc WHERE id=?";
	MYSQL_STMT* stmt_b = mysql_stmt_init(proxy);
	if (!stmt_b) {
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_future_gtid: mysql_stmt_init() failed");
	}

	if (mysql_stmt_prepare(stmt_b, query_b.c_str(), query_b.length()) != 0) {
		std::string stmt_err = mysql_stmt_error(stmt_b) ? mysql_stmt_error(stmt_b) : "(null)";
		mysql_stmt_close(stmt_b);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_future_gtid: mysql_stmt_prepare() failed %s", stmt_err.c_str());
	}

	MYSQL_BIND bind_b;
	memset(&bind_b, 0, sizeof(bind_b));
	int id_val = 1;
	unsigned long len = 0;
	my_bool is_null = 0;
	bind_b.buffer_type = MYSQL_TYPE_LONG;
	bind_b.buffer = &id_val;
	bind_b.buffer_length = sizeof(id_val);
	bind_b.length = &len;
	bind_b.is_null = &is_null;

	if (mysql_stmt_bind_param(stmt_b, &bind_b) != 0) {
		std::string stmt_err = mysql_stmt_error(stmt_b) ? mysql_stmt_error(stmt_b) : "(null)";
		mysql_stmt_close(stmt_b);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_future_gtid: mysql_stmt_bind_param() failed: %s", stmt_err.c_str());
	}

	int rc_execute_b = mysql_stmt_execute(stmt_b);
	unsigned int errno_b = mysql_stmt_errno(stmt_b);
	const char* errmsg_b = mysql_stmt_error(stmt_b);

	ok(rc_execute_b != 0, "test_future_gtid: Execute with future GTID should fail");
	ok(errno_b == 9001, "test_future_gtid: Error code should be 9001 (timeout), got %u", errno_b);

	std::string err_str(errmsg_b ? errmsg_b : "");
	bool has_timeout_msg = (err_str.find("Max connect timeout") != std::string::npos);
	ok(has_timeout_msg, "test_future_gtid: Error message should contain 'Max connect timeout': %s", errmsg_b ? errmsg_b : "(null)");

	std::string query_text_b;
	long global_stmt_id_b = 0;
	if (check_ps_cache(admin, "test.ps_min_gtid_fc", query_text_b, global_stmt_id_b) == 0) {
		ok(query_text_b.find("min_gtid=") == std::string::npos,
		   "test_future_gtid: Cached query should not contain min_gtid annotation (rewritten): %s", query_text_b.c_str());
	} else {
		mysql_stmt_close(stmt_b);
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("test_future_gtid: Failed to check PS cache");
	}

	if (global_stmt_id_a == global_stmt_id_b) {
		ok(true, "Both tests use the same global_stmt_id=%ld (cached PS reuse)", global_stmt_id_a);
	} else {
		ok(false, "Different global_stmt_id: test_1=%ld, test_2=%ld", global_stmt_id_a, global_stmt_id_b);
	}

	mysql_stmt_close(stmt_b);
}

int main(int, char**) {
	CommandLine cl;
	if (cl.getEnv()) {
		diag("Failed to get required environmental variables");
		return -1;
	}

	plan(6);

	MYSQL* admin = init_mysql_conn(cl.host, cl.admin_port, cl.admin_username, cl.admin_password);
	if (!admin) {
		fprintf(stderr, "File %s, line %d, Error: %s\n", __FILE__, __LINE__, mysql_error(admin));
		return exit_status();
	}

	MYSQL* proxy = init_mysql_conn(cl.host, cl.port, cl.username, cl.password);
	if (!proxy) {
		fprintf(stderr, "File %s, line %d, Error: %s\n", __FILE__, __LINE__, mysql_error(proxy));
		mysql_close(admin);
		return exit_status();
	}

	setup(admin, proxy, cl);

	std::string server_uuid;
	uint64_t max_trxid = 0;
	if (get_gtid_info(admin, cl, server_uuid, max_trxid) != 0) {
		mysql_close(proxy);
		mysql_close(admin);
		BAIL_OUT("No GTID info available from stats.stats_mysql_gtid_executed for backend %s:%d",
			cl.mysql_host, cl.mysql_port);
	}

	std::string current_gtid = server_uuid + ":" + std::to_string(max_trxid);
	std::string future_gtid = server_uuid + ":" + std::to_string(max_trxid + 100000);

	diag("Current GTID: %s", current_gtid.c_str());
	diag("Future GTID:  %s", future_gtid.c_str());
	long global_stmt_id_a = test_prepare_stmt_valid_gtid(admin, proxy, current_gtid);
	test_prepare_stmt_future_gtid(admin, proxy, future_gtid, global_stmt_id_a);

	cleanup(admin);

	mysql_close(proxy);
	mysql_close(admin);

	return exit_status();
}