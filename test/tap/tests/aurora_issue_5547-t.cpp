#include <stdio.h>
#include <string>

#include "mysql.h"
#include "proxysql_utils.h"
#include "tap.h"
#include "command_line.h"
#include "utils.h"

static int test_column_exists(MYSQL* admin) {
	const char* query = "SELECT name FROM pragma_table_info('mysql_aws_aurora_hostgroups') WHERE name='autopurge_missing_checks'";
	if (mysql_query(admin, query)) {
		fprintf(stderr, "Error querying table info: %s\n", mysql_error(admin));
		return EXIT_FAILURE;
	}
	MYSQL_RES* res = mysql_store_result(admin);
	const int num_rows = mysql_num_rows(res);
	mysql_free_result(res);
	ok(num_rows == 1, "autopurge_missing_checks column exists");
	return (num_rows == 1) ? EXIT_SUCCESS : EXIT_FAILURE;
}

static int test_value_roundtrip(MYSQL* admin) {
	const char* insert_query =
		"INSERT OR REPLACE INTO mysql_aws_aurora_hostgroups "
		"(writer_hostgroup, reader_hostgroup, domain_name, autopurge_missing_checks) "
		"VALUES (9998, 9999, '.test.local', 3)";
	if (mysql_query(admin, insert_query)) {
		fprintf(stderr, "Error inserting: %s\n", mysql_error(admin));
		return EXIT_FAILURE;
	}

	const char* select_query =
		"SELECT autopurge_missing_checks FROM mysql_aws_aurora_hostgroups WHERE writer_hostgroup=9998";
	if (mysql_query(admin, select_query)) {
		fprintf(stderr, "Error selecting: %s\n", mysql_error(admin));
		return EXIT_FAILURE;
	}
	MYSQL_RES* res = mysql_store_result(admin);
	MYSQL_ROW row = mysql_fetch_row(res);
	const int value = row ? atoi(row[0]) : -1;
	mysql_free_result(res);
	ok(value == 3, "autopurge_missing_checks can be configured");

	const char* delete_query = "DELETE FROM mysql_aws_aurora_hostgroups WHERE writer_hostgroup=9998";
	mysql_query(admin, delete_query);
	return (value == 3) ? EXIT_SUCCESS : EXIT_FAILURE;
}

static int test_default_value(MYSQL* admin) {
	const char* insert_query =
		"INSERT OR REPLACE INTO mysql_aws_aurora_hostgroups "
		"(writer_hostgroup, reader_hostgroup, domain_name) "
		"VALUES (9997, 9996, '.test.local')";
	if (mysql_query(admin, insert_query)) {
		fprintf(stderr, "Error inserting: %s\n", mysql_error(admin));
		return EXIT_FAILURE;
	}

	const char* select_query =
		"SELECT autopurge_missing_checks FROM mysql_aws_aurora_hostgroups WHERE writer_hostgroup=9997";
	if (mysql_query(admin, select_query)) {
		fprintf(stderr, "Error selecting: %s\n", mysql_error(admin));
		return EXIT_FAILURE;
	}
	MYSQL_RES* res = mysql_store_result(admin);
	MYSQL_ROW row = mysql_fetch_row(res);
	const int value = row ? atoi(row[0]) : -1;
	mysql_free_result(res);
	ok(value == 0, "autopurge_missing_checks defaults to 0");

	const char* delete_query = "DELETE FROM mysql_aws_aurora_hostgroups WHERE writer_hostgroup=9997";
	mysql_query(admin, delete_query);
	return (value == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

static int test_valid_range(MYSQL* admin) {
	const char* insert_query =
		"INSERT OR REPLACE INTO mysql_aws_aurora_hostgroups "
		"(writer_hostgroup, reader_hostgroup, domain_name, autopurge_missing_checks) "
		"VALUES (9995, 9994, '.test.local', 100)";
	const int rc = mysql_query(admin, insert_query);
	ok(rc == 0, "autopurge_missing_checks accepts upper boundary value 100");
	if (rc == 0) {
		const char* delete_query = "DELETE FROM mysql_aws_aurora_hostgroups WHERE writer_hostgroup=9995";
		mysql_query(admin, delete_query);
	}
	return (rc == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

int main(int argc, char** argv) {
	CommandLine cl;
	if (cl.getEnv()) {
		fprintf(stderr, "Failed to get environment variables\n");
		return EXIT_FAILURE;
	}

	plan(4);

	MYSQL* admin = mysql_init(NULL);
	if (!mysql_real_connect(admin, cl.host, cl.admin_username, cl.admin_password, NULL, cl.admin_port, NULL, 0)) {
		fprintf(stderr, "Failed to connect to ProxySQL Admin: %s\n", mysql_error(admin));
		return EXIT_FAILURE;
	}

	test_column_exists(admin);
	test_value_roundtrip(admin);
	test_default_value(admin);
	test_valid_range(admin);

	mysql_close(admin);
	return exit_status();
}
