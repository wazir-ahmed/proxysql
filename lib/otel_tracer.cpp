#include "otel_tracer.h"
#include "cpp.h"
#include <cstdint>
#include <cstdio>
#include <inttypes.h>

#define OTEL_SERVICE_NAME_DEFAULT		"proxysql"
#define OTEL_OTLP_PROTO_DEFAULT			"http/protobuf"
#define OTEL_OTLP_ENDPOINT_DEFAULT		"http://127.0.0.1:4318"
#define OTEL_OTLP_COMPRESSION_DEFAULT	"none"

using std::string;

const std::vector<string> otel_variables = {
	"trace_enable",
	"trace_service_name",
	"trace_resource_attributes",
	"trace_exporter_otlp_protocol",
	"trace_exporter_otlp_endpoint",
	"trace_exporter_otlp_headers",
	"trace_exporter_otlp_certificates",
	"trace_exporter_otlp_compression",
	"trace_exporter_otlp_timeout",
	"trace_bsp_schedule_delay",
	"trace_bsp_max_queue_size",
	"trace_bsp_max_export_batch_size"
};

const std::vector<string> otlp_protocol = {
	"http/protobuf",
	// "http/json",
	// "grpc",
};

const std::vector<string> otlp_compression = {
	"none",
	"gzip"
};

OTelTracer::OTelTracer() : variables{} {
	pthread_rwlock_init(&rwlock,NULL);

	variables.service_name = strdup(OTEL_SERVICE_NAME_DEFAULT);
	variables.exporter_otlp_protocol = strdup(OTEL_OTLP_PROTO_DEFAULT);
	variables.exporter_otlp_endpoint = strdup(OTEL_OTLP_ENDPOINT_DEFAULT);
	variables.exporter_otlp_compression = strdup(OTEL_OTLP_COMPRESSION_DEFAULT);
}

OTelTracer::~OTelTracer() {
	free(variables.service_name);
	free(variables.resource_attributes);
	free(variables.exporter_otlp_protocol);
	free(variables.exporter_otlp_endpoint);
	free(variables.exporter_otlp_headers);
	free(variables.exporter_otlp_certificates);
	free(variables.exporter_otlp_compression);

	pthread_rwlock_destroy(&rwlock);
}

void OTelTracer::init() {}

void OTelTracer::rdlock() {
	pthread_rwlock_rdlock(&rwlock);
}

void OTelTracer::wrlock() {
	pthread_rwlock_wrlock(&rwlock);
}

void OTelTracer::unlock() {
	pthread_rwlock_unlock(&rwlock);
}

const std::vector<std::string>& OTelTracer::get_variables_list() {
	return otel_variables;
}

bool OTelTracer::has_variable(const char * key) {
	for (auto& var : otel_variables) {
		if (strcasecmp(var.c_str(), key) == 0) {
			return true;
		}
	}

	return false;
}

char * OTelTracer::get_variable(const char* key) {
	char buf[21];

	if (strcasecmp(key,"trace_enable") == 0)
		return strdup(variables.trace_enable ? "true" : "false");

	if (strcasecmp(key,"trace_service_name") == 0)
		return s_strdup(variables.service_name);

	if (strcasecmp(key,"trace_resource_attributes") == 0)
		return s_strdup(variables.resource_attributes);

	if (strcasecmp(key,"trace_exporter_otlp_protocol") == 0)
		return s_strdup(variables.exporter_otlp_protocol);

	if (strcasecmp(key,"trace_exporter_otlp_endpoint") == 0)
		return s_strdup(variables.exporter_otlp_endpoint);

	if (strcasecmp(key,"trace_exporter_otlp_headers") == 0)
		return s_strdup(variables.exporter_otlp_headers);

	if (strcasecmp(key,"trace_exporter_otlp_certificates") == 0)
		return s_strdup(variables.exporter_otlp_certificates);

	if (strcasecmp(key,"trace_exporter_otlp_compression") == 0)
		return s_strdup(variables.exporter_otlp_compression);

	if (strcasecmp(key,"trace_exporter_otlp_timeout") == 0) {
		if (variables.exporter_otlp_timeout > 0) {
			sprintf(buf, "%" PRId64, variables.exporter_otlp_timeout);
			return strdup(buf);
		}
		return nullptr;
	}

	if (strcasecmp(key,"trace_bsp_schedule_delay") == 0) {
		if (variables.bsp_schedule_delay > 0) {
			sprintf(buf, "%" PRId64, variables.bsp_schedule_delay);
			return strdup(buf);
		}
		return nullptr;
	}

	if (strcasecmp(key,"trace_bsp_max_queue_size") == 0) {
		if (variables.bsp_max_queue_size > 0) {
			sprintf(buf, "%zu", variables.bsp_max_queue_size);
			return strdup(buf);
		}
		return nullptr;
	}

	if (strcasecmp(key,"trace_bsp_max_export_batch_size") == 0) {
		if (variables.bsp_max_export_batch_size > 0) {
			sprintf(buf, "%zu", variables.bsp_max_export_batch_size);
			return strdup(buf);
		}
		return nullptr;
	}

	return NULL;
}

bool OTelTracer::set_variable(const char *key, const char *val) {
	if (strcasecmp(key,"trace_enable") == 0) {
		if (strcasecmp(val, "true") == 0 || strcasecmp(val, "1") == 0) {
			variables.trace_enable = true;
			return true;
		}
		if (strcasecmp(val, "false") == 0 || strcasecmp(val, "0") == 0) {
			variables.trace_enable = false;
			return true;
		}
		return false;
	}

	if (strcasecmp(key,"trace_service_name") == 0) {
		if (strlen(val) > 0) {
			free(variables.service_name);
			variables.service_name = strdup(val);
			return true;
		}
		return false;
	}

	if (strcasecmp(key,"trace_resource_attributes") == 0) {
		free(variables.resource_attributes);
		variables.resource_attributes = strdup(val);
		return true;
	}

	if (strcasecmp(key,"trace_exporter_otlp_protocol") == 0) {
		for (auto& protocol : otlp_protocol) {
			if (strcasecmp(val, protocol.c_str()) == 0) {
				free(variables.exporter_otlp_protocol);
				variables.exporter_otlp_protocol = strdup(val);
				return true;
			}
		}
		return false;
	}

	if (strcasecmp(key,"trace_exporter_otlp_endpoint") == 0) {
		if (strlen(val) > 0) {
			free(variables.exporter_otlp_endpoint);
			variables.exporter_otlp_endpoint = strdup(val);
			return true;
		}
		return false;
	}

	if (strcasecmp(key,"trace_exporter_otlp_headers") == 0) {
		free(variables.resource_attributes);
		variables.resource_attributes = strdup(val);
		return true;
	}

	if (strcasecmp(key,"trace_exporter_otlp_certificates") == 0) {
		if (strlen(val) > 0) {
			free(variables.exporter_otlp_certificates);
			variables.exporter_otlp_certificates = strdup(val);
			return true;
		}
		return false;
	}

	if (strcasecmp(key,"trace_exporter_otlp_compression") == 0) {
		for (auto& algo : otlp_compression) {
			if (strcasecmp(val, algo.c_str()) == 0) {
				free(variables.exporter_otlp_compression);
				variables.exporter_otlp_compression = strdup(val);
				return true;
			}
		}
		return false;
	}

	if (strcasecmp(key,"trace_exporter_otlp_timeout") == 0) {
		int64_t timeout = strtoll(val, NULL, 10);
		if (timeout > 0) {
			variables.exporter_otlp_timeout = timeout;
			return true;
		}
		return false;
	}

	if (strcasecmp(key,"trace_bsp_schedule_delay") == 0) {
		int64_t delay = strtoll(val, NULL, 10);
		if (delay > 0) {
			variables.bsp_schedule_delay = delay;
			return true;
		}
		return false;
	}

	if (strcasecmp(key,"trace_bsp_max_queue_size") == 0) {
		size_t qsize = strtol(val, NULL, 10);
		if (qsize > 0) {
			variables.bsp_max_queue_size = qsize;
			return true;
		}
		return false;
	}

	if (strcasecmp(key,"trace_bsp_max_export_batch_size") == 0) {
		size_t bsize = strtol(val, NULL, 10);
		if (bsize > 0) {
			variables.bsp_max_export_batch_size = bsize;
			return true;
		}
		return false;
	}

	return false;
}
