#include <fstream>
#include <sstream>
#include <string>

#include "tap.h"

static std::string read_file(const char* path) {
	std::ifstream input(path);
	std::ostringstream buffer;
	buffer << input.rdbuf();
	return buffer.str();
}

int main() {
	plan(3);

	const std::string source = read_file("../../../lib/MySQL_HostGroups_Manager.cpp");
	ok(source.empty() == false, "loaded MySQL_HostGroups_Manager.cpp");
	ok(source.find("t = t + (2 * b - a);") != std::string::npos,
		"issue #5546 safety margin uses 2 * ping interval");
	ok(source.find("if (b > a)") != std::string::npos,
		"issue #5546 compensation remains gated by monitor interval comparison");

	return exit_status();
}
