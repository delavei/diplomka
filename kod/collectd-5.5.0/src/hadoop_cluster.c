#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

#include "common.h"

#include "plugin.h"
#include "cJSON.h"

#define PLUGIN_NAME "hadoop_cluster"
#define TAGS_SIZE 1024
#define CLUSTER_SEARCH_SUBSTR "clusterMetrics\":"
#define CLUSTER_INFO_SEARCH_SUBSTR "clusterInfo\":"

char* yarn_url;
char* yarn_port;

static const char *config_keys[] = {
    "YARNUrl",
    "Port"
};
static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);
 

CURL* easy_handle;
int new_server_read = 1;
int read_success = -1;
int read_info_success = -1;
long int cluster_id;

static void init_value_list (value_list_t *vl)
{
    sstrncpy (vl->plugin, PLUGIN_NAME, sizeof (vl->plugin));

	vl->meta = meta_data_create();
	
}

void submit_cluster_value (unsigned long value, const char* type_instance, const char* tags) {
	value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].derive = value;

    vl.values = values;
    vl.values_len = 1;

	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, "cluster", sizeof (vl.type));
    sstrncpy (vl.type_instance, type_instance, sizeof (vl.type_instance));
    
    plugin_dispatch_values (&vl);
}

static int submit_cluster_stats (char* cluster_json) {
	cJSON* root = cJSON_Parse(cluster_json);
	
	if (root == NULL) {
		return -1;
	}
	cJSON* cluster_metrics_node = cJSON_GetObjectItem(root,"clusterMetrics");
	if (cluster_metrics_node == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"clusterMetrics\" node from json.");
		return -1;
	}
	cJSON* apps_submitted = cJSON_GetObjectItem(cluster_metrics_node,"appsSubmitted");
	if (apps_submitted == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"appsSubmitted\" node from json.");
		return -1;
	}
	cJSON* apps_completed = cJSON_GetObjectItem(cluster_metrics_node,"appsCompleted");
	if (apps_completed == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"appsCompleted\" node from json.");
		return -1;
	}
	cJSON* apps_pending = cJSON_GetObjectItem(cluster_metrics_node,"appsPending");
	if (apps_completed == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"appsCompleted\" node from json.");
		return -1;
	}
	cJSON* apps_running = cJSON_GetObjectItem(cluster_metrics_node,"appsRunning");
	if (apps_running == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"appsCompleted\" node from json.");
		return -1;
	}
	cJSON* apps_failed = cJSON_GetObjectItem(cluster_metrics_node,"appsFailed");
	if (apps_failed == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"appsCompleted\" node from json.");
		return -1;
	}
	cJSON* apps_killed = cJSON_GetObjectItem(cluster_metrics_node,"appsKilled");
	if (apps_killed == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"appsKilled\" node from json.");
		return -1;
	}
	cJSON* reserved_mb = cJSON_GetObjectItem(cluster_metrics_node,"reservedMB");
	if (reserved_mb == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"reservedMB\" node from json.");
		return -1;
	}
	cJSON* available_mb = cJSON_GetObjectItem(cluster_metrics_node,"availableMB");
	if (available_mb == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"availableMB\" node from json.");
		return -1;
	}
	cJSON* allocated_mb = cJSON_GetObjectItem(cluster_metrics_node,"allocatedMB");
	if (allocated_mb == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"allocatedMB\" node from json.");
		return -1;
	}
	cJSON* total_mb = cJSON_GetObjectItem(cluster_metrics_node,"totalMB");
	if (total_mb == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"totalMB\" node from json.");
		return -1;
	}
	cJSON* reserved_virtual_cores = cJSON_GetObjectItem(cluster_metrics_node,"reservedVirtualCores");
	if (reserved_virtual_cores == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"reservedVirtualCores\" node from json.");
		return -1;
	}
	cJSON* available_virtual_cores = cJSON_GetObjectItem(cluster_metrics_node,"availableVirtualCores");
	if (available_virtual_cores == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"availableVirtualCores\" node from json.");
		return -1;
	}
	cJSON* allocated_virtual_cores = cJSON_GetObjectItem(cluster_metrics_node,"allocatedVirtualCores");
	if (allocated_virtual_cores == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"allocatedVirtualCores\" node from json.");
		return -1;
	}
	cJSON* total_virtual_cores = cJSON_GetObjectItem(cluster_metrics_node,"totalVirtualCores");
	if (total_virtual_cores == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"totalVirtualCores\" node from json.");
		return -1;
	}
	cJSON* total_nodes = cJSON_GetObjectItem(cluster_metrics_node,"totalNodes");
	if (total_nodes == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"totalNodes\" node from json.");
		return -1;
	}
	cJSON* active_nodes = cJSON_GetObjectItem(cluster_metrics_node,"activeNodes");
	if (active_nodes == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"activeNodes\" node from json.");
		return -1;
	}
	cJSON* lost_nodes = cJSON_GetObjectItem(cluster_metrics_node,"lostNodes");
	if (lost_nodes == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"lostNodes\" node from json.");
		return -1;
	}
	cJSON* unhealthy_nodes = cJSON_GetObjectItem(cluster_metrics_node,"unhealthyNodes");
	if (unhealthy_nodes == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"unhealthyNodes\" node from json.");
		return -1;
	}
	cJSON* decommissioned_nodes = cJSON_GetObjectItem(cluster_metrics_node,"decommissionedNodes");
	if (decommissioned_nodes == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"decommissionedNodes\" node from json.");
		return -1;
	}
	cJSON* rebooted_nodes = cJSON_GetObjectItem(cluster_metrics_node,"rebootedNodes");
	if (rebooted_nodes == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"rebootedNodes\" node from json.");
		return -1;
	}
		
	char tags[TAGS_SIZE];
	ssnprintf(tags,TAGS_SIZE,"cluster_id=%ld", cluster_id);
	
	submit_cluster_value (apps_submitted->valueint, "apps_submitted", tags);
	submit_cluster_value (apps_completed->valueint, "apps_completed", tags);
	submit_cluster_value (apps_pending->valueint, "apps_pending", tags);
	submit_cluster_value (apps_running->valueint, "apps_running", tags);
	submit_cluster_value (apps_failed->valueint, "apps_failed", tags);
	submit_cluster_value (apps_killed->valueint, "apps_killed", tags);
	submit_cluster_value (reserved_mb->valueint, "reserved_mb", tags);
	submit_cluster_value (available_mb->valueint, "available_mb", tags);
	submit_cluster_value (allocated_mb->valueint, "allocated_mb", tags);
	submit_cluster_value (total_mb->valueint, "total_mb", tags);
	submit_cluster_value (reserved_virtual_cores->valueint, "reserved_virtual_cores", tags);
	submit_cluster_value (available_virtual_cores->valueint, "available_virtual_cores", tags);
	submit_cluster_value (allocated_virtual_cores->valueint, "allocated_virtual_cores", tags);
	submit_cluster_value (total_virtual_cores->valueint, "total_virtual_cores", tags);
	submit_cluster_value (total_nodes->valueint, "total_nodes", tags);
	submit_cluster_value (active_nodes->valueint, "active_nodes", tags);
	submit_cluster_value (lost_nodes->valueint, "lost_nodes", tags);
	submit_cluster_value (unhealthy_nodes->valueint, "unhealthy_nodes", tags);
	submit_cluster_value (decommissioned_nodes->valueint, "decommissioned_nodes", tags);
	submit_cluster_value (rebooted_nodes->valueint, "rebooted_nodes", tags);
	
	cJSON_Delete(root);
	
	return 0;
}

size_t read_response(char *data, size_t size, size_t nmemb, void *userdata) {
	size_t retval = nmemb*size;
	char* cluster_substr = strstr(data, CLUSTER_SEARCH_SUBSTR);
	
	if (cluster_substr) {
		if (submit_cluster_stats(data) == 0) {
			read_success = 0;								
		}
	}	
	
	return retval;
}

size_t read_response_cluster_info(char *data, size_t size, size_t nmemb, void *userdata) {
	size_t retval = nmemb*size;
	char* cluster_info_substr = strstr(data, CLUSTER_INFO_SEARCH_SUBSTR);
	
	if (cluster_info_substr) {
		cJSON* root = cJSON_Parse(data);

		if (root == NULL) {
			return retval;
		} else {
			cJSON* clusterInfo = cJSON_GetObjectItem(root,"clusterInfo");
			if (clusterInfo == NULL) {
				ERROR (PLUGIN_NAME " plugin: error getting \"clusterInfo\" node cluster json.");
				return -1;
			}
			cJSON* id = cJSON_GetObjectItem(clusterInfo,"id");
			if (id == NULL) {
				ERROR (PLUGIN_NAME " plugin: error getting \"id\" node cluster json.");
				return -1;
			}
			cluster_id = (long int) id->valuedouble;
			read_info_success = 0;
		}
	}
	
	return retval;
}

static int hadoop_cluster_read (void)
{
	CURLcode curl_ret = curl_easy_perform(easy_handle);
	if (curl_ret == CURLE_OK) {
		new_server_read = 1;
		if (read_success >= 0) {
			return 0;
		} else {
			ERROR(PLUGIN_NAME " plugin: cluster metrics not found in response.");
			return -1;
		}
	} else {
		ERROR(PLUGIN_NAME " plugin: error reading cluster metrics, curl errno %d.",curl_ret);
		return -1;
	}
	
	return 0;
}

static int hadoop_cluster_shutdown (void)
{
	curl_easy_cleanup(easy_handle);  
    return 0;
}

static int hadoop_cluster_init (void)
{		
	easy_handle = curl_easy_init();
	curl_easy_setopt(easy_handle, CURLOPT_HTTPAUTH, (long)CURLAUTH_GSSNEGOTIATE);
	curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, read_response);
    curl_easy_setopt(easy_handle, CURLOPT_USERAGENT, "curl/7.50.1");	
	curl_easy_setopt(easy_handle, CURLOPT_USERPWD, ":");        
    curl_easy_setopt(easy_handle, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(easy_handle, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(easy_handle, CURLOPT_FOLLOWLOCATION, "true");
    curl_easy_setopt(easy_handle, CURLOPT_MAXREDIRS, 50L);
	
	char cluster_url[256];
	snprintf(cluster_url, 256, "%s:%s/ws/v1/cluster/info", yarn_url, yarn_port);
	curl_easy_setopt(easy_handle, CURLOPT_URL, cluster_url);
	curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, read_response_cluster_info);
	
	CURLcode curl_ret = curl_easy_perform(easy_handle);
	if (curl_ret == CURLE_OK) {
		if (read_info_success != 0) {
			ERROR(PLUGIN_NAME " plugin: cluster info not found in response.");
			return -1;
		}
	} else {
		ERROR(PLUGIN_NAME " plugin: error reading cluster info, curl errno %d.",curl_ret);
		return -1;
	}
	
	snprintf(cluster_url, 256, "%s:%s/ws/v1/cluster/metrics", yarn_url, yarn_port);
	curl_easy_setopt(easy_handle, CURLOPT_URL, cluster_url);
	curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, read_response);

	return 0;
}

static int hadoop_cluster_config (const char *key, const char *value)
{
    if (strcasecmp (key, "YARNUrl") == 0) {
        yarn_url = strdup (value);
        if (yarn_url == NULL) {
            ERROR (PLUGIN_NAME " plugin: YARNUrl strdup failed.");
            return -1;
        }
    }
	
	if (strcasecmp (key, "Port") == 0) {
        yarn_port = strdup (value);
        if (yarn_port == NULL) {
            ERROR (PLUGIN_NAME " plugin: Port strdup failed.");
            return -1;
        }
        return 0;
    }

    /* Unrecognised option. */
    return -1;
}

void module_register (void)
{
	plugin_register_read (PLUGIN_NAME, hadoop_cluster_read);
	plugin_register_config(PLUGIN_NAME, hadoop_cluster_config, config_keys, config_keys_num);
	plugin_register_init (PLUGIN_NAME, hadoop_cluster_init);
	plugin_register_shutdown (PLUGIN_NAME, hadoop_cluster_shutdown);
    return;
}

