/**
 * @file test_com_register_slave_enables_fast_forward-t.cpp
 * @brief Test COM_REGISTER_SLAVE enables fast forward.
 * @details Verifies that ProxySQL correctly enables fast forward for a user
 *   when it receives COM_REGISTER_SLAVE, even if fast_forward was initially
 *   disabled. Uses a native in-tree helper (via MARIADB_RPL) instead of the
 *   former external test_binlog_reader-t binary.
 */

#include <cstdlib>
#include "tap.h"
#include "command_line.h"
#include "native_test_binlog_reader.h"

int main(int argc, char** argv) {
	CommandLine cl;

	plan(1);
	diag("Testing COM_REGISTER_SLAVE enables fast forward");
	diag("This test verifies that ProxySQL correctly enables fast forward for a"
		" user when it receives COM_REGISTER_SLAVE, even if it was initially disabled.");

	if (cl.getEnv()) {
		diag("Failed to get the required environmental variables.");
		return EXIT_FAILURE;
	}

	const int native_res = run_native_test_binlog_reader(cl);

	if (native_res != 0) {
		diag("Native test_binlog_reader failed with exit code: %d", native_res);
	}

	ok(
		native_res == 0,
		"Native test_binlog_reader should complete successfully. Err code was: %d",
		native_res
	);

	diag("Test completed");
	return exit_status();
}
